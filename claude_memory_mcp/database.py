"""Database layer for Claude memory — SQLite with optional Turso upgrade path."""

import sqlite3
import os
import secrets
from dataclasses import dataclass, field
from pathlib import Path

SCHEMA = """\
CREATE TABLE IF NOT EXISTS memories (
    id          TEXT PRIMARY KEY,
    project     TEXT NOT NULL,
    topic       TEXT NOT NULL,
    key         TEXT NOT NULL,
    content     TEXT NOT NULL,
    tags        TEXT NOT NULL DEFAULT '[]',
    source      TEXT NOT NULL,
    confidence  REAL NOT NULL DEFAULT 1.0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    supersedes  TEXT,
    archived    INTEGER NOT NULL DEFAULT 0,
    UNIQUE(project, topic, key)
);

CREATE TABLE IF NOT EXISTS write_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    memory_id   TEXT NOT NULL,
    action      TEXT NOT NULL,
    instance    TEXT NOT NULL,
    timestamp   TEXT NOT NULL DEFAULT (datetime('now')),
    old_content TEXT,
    new_content TEXT
);

CREATE INDEX IF NOT EXISTS idx_memories_project ON memories(project, archived);
CREATE INDEX IF NOT EXISTS idx_memories_topic ON memories(project, topic, archived);
CREATE INDEX IF NOT EXISTS idx_memories_key ON memories(project, key, archived);
CREATE INDEX IF NOT EXISTS idx_write_log_memory ON write_log(memory_id);
CREATE INDEX IF NOT EXISTS idx_write_log_timestamp ON write_log(timestamp);
"""


def _gen_id() -> str:
    return secrets.token_hex(8)


@dataclass
class Memory:
    id: str
    project: str
    topic: str
    key: str
    content: str
    tags: list[str]
    source: str
    confidence: float
    created_at: str
    updated_at: str
    supersedes: str | None
    archived: int


@dataclass
class WriteLogEntry:
    id: int
    memory_id: str
    action: str
    instance: str
    timestamp: str
    old_content: str | None
    new_content: str | None


class MemoryDatabase:
    def __init__(self, db_path: str | None = None):
        path = db_path or os.environ.get("DATABASE_PATH", "claude_memory.db")
        # Resolve relative paths against a stable location
        if not os.path.isabs(path):
            data_dir = Path.home() / ".claude-memory"
            data_dir.mkdir(exist_ok=True)
            path = str(data_dir / path)
        self.db_path = path
        self._conn: sqlite3.Connection | None = None

    @property
    def conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def initialize(self) -> None:
        cursor = self.conn.cursor()
        cursor.executescript(SCHEMA)
        self.conn.commit()

    def read(
        self,
        project: str,
        topic: str | None = None,
        key: str | None = None,
        tags: list[str] | None = None,
    ) -> list[Memory]:
        sql = "SELECT * FROM memories WHERE project = ? AND archived = 0"
        args: list[str] = [project]

        if topic:
            sql += " AND topic = ?"
            args.append(topic)
        if key:
            sql += " AND key = ?"
            args.append(key)
        if tags:
            for tag in tags:
                sql += " AND tags LIKE ?"
                args.append(f'%"{tag}"%')

        sql += " ORDER BY updated_at DESC"

        rows = self.conn.execute(sql, args).fetchall()
        return [self._row_to_memory(r) for r in rows]

    def write(
        self,
        project: str,
        topic: str,
        key: str,
        content: str,
        source: str,
        tags: list[str] | None = None,
        confidence: float = 1.0,
    ) -> tuple[str, str]:
        """Returns (action, memory_id)."""
        import json

        tags_json = json.dumps(tags or [])

        # Check existing
        row = self.conn.execute(
            "SELECT id, content FROM memories WHERE project = ? AND topic = ? AND key = ?",
            (project, topic, key),
        ).fetchone()

        if row:
            memory_id = row["id"]
            old_content = row["content"]
            action = "updated"
            self.conn.execute(
                """UPDATE memories
                   SET content = ?, tags = ?, source = ?, confidence = ?,
                       updated_at = datetime('now'), archived = 0
                   WHERE id = ?""",
                (content, tags_json, source, confidence, memory_id),
            )
        else:
            memory_id = _gen_id()
            old_content = None
            action = "created"
            self.conn.execute(
                """INSERT INTO memories (id, project, topic, key, content, tags, source, confidence)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (memory_id, project, topic, key, content, tags_json, source, confidence),
            )

        # Write log
        self.conn.execute(
            """INSERT INTO write_log (memory_id, action, instance, old_content, new_content)
               VALUES (?, ?, ?, ?, ?)""",
            (memory_id, "create" if action == "created" else "update", source, old_content, content),
        )

        self.conn.commit()
        return action, memory_id

    def search(self, query: str, project: str | None = None) -> list[Memory]:
        pattern = f"%{query}%"
        sql = "SELECT * FROM memories WHERE archived = 0 AND (content LIKE ? OR key LIKE ? OR topic LIKE ?)"
        args: list[str] = [pattern, pattern, pattern]

        if project:
            sql += " AND project = ?"
            args.append(project)

        sql += " ORDER BY updated_at DESC LIMIT 50"

        rows = self.conn.execute(sql, args).fetchall()
        return [self._row_to_memory(r) for r in rows]

    def history(
        self,
        project: str | None = None,
        key: str | None = None,
        limit: int = 50,
    ) -> list[WriteLogEntry]:
        if key and project:
            sql = """SELECT wl.* FROM write_log wl
                     JOIN memories m ON wl.memory_id = m.id
                     WHERE m.project = ? AND m.key = ?
                     ORDER BY wl.timestamp DESC LIMIT ?"""
            args: list[str | int] = [project, key, limit]
        elif project:
            sql = """SELECT wl.* FROM write_log wl
                     JOIN memories m ON wl.memory_id = m.id
                     WHERE m.project = ?
                     ORDER BY wl.timestamp DESC LIMIT ?"""
            args = [project, limit]
        else:
            sql = "SELECT * FROM write_log ORDER BY timestamp DESC LIMIT ?"
            args = [limit]

        rows = self.conn.execute(sql, args).fetchall()
        return [
            WriteLogEntry(
                id=r["id"],
                memory_id=r["memory_id"],
                action=r["action"],
                instance=r["instance"],
                timestamp=r["timestamp"],
                old_content=r["old_content"],
                new_content=r["new_content"],
            )
            for r in rows
        ]

    def archive(self, memory_id: str, source: str) -> bool:
        row = self.conn.execute(
            "SELECT id, content FROM memories WHERE id = ?", (memory_id,)
        ).fetchone()

        if not row:
            return False

        self.conn.execute(
            "UPDATE memories SET archived = 1, updated_at = datetime('now') WHERE id = ?",
            (memory_id,),
        )
        self.conn.execute(
            """INSERT INTO write_log (memory_id, action, instance, old_content, new_content)
               VALUES (?, 'archive', ?, ?, NULL)""",
            (memory_id, source, row["content"]),
        )
        self.conn.commit()
        return True

    def list_memories(self, project: str | None = None) -> list[dict]:
        if project:
            sql = """SELECT project, topic, key, updated_at FROM memories
                     WHERE archived = 0 AND project = ?
                     ORDER BY project, topic, key"""
            rows = self.conn.execute(sql, (project,)).fetchall()
        else:
            sql = """SELECT project, topic, key, updated_at FROM memories
                     WHERE archived = 0
                     ORDER BY project, topic, key"""
            rows = self.conn.execute(sql).fetchall()

        return [
            {
                "project": r["project"],
                "topic": r["topic"],
                "key": r["key"],
                "updated_at": r["updated_at"],
            }
            for r in rows
        ]

    @staticmethod
    def _row_to_memory(row: sqlite3.Row) -> Memory:
        import json

        return Memory(
            id=row["id"],
            project=row["project"],
            topic=row["topic"],
            key=row["key"],
            content=row["content"],
            tags=json.loads(row["tags"]),
            source=row["source"],
            confidence=row["confidence"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            supersedes=row["supersedes"],
            archived=row["archived"],
        )
