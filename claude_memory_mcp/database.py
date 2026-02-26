"""Database layer for Claude memory — SQLite local or Turso remote."""

import json
import sqlite3
import os
import secrets
from dataclasses import dataclass
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


class DatabaseBackend:
    """Abstract interface for database operations."""

    def execute(self, sql: str, args: list | None = None) -> list[dict]:
        raise NotImplementedError

    def executescript(self, script: str) -> None:
        raise NotImplementedError

    def commit(self) -> None:
        pass


class SQLiteBackend(DatabaseBackend):
    """Local SQLite backend."""

    def __init__(self, path: str):
        self._conn = sqlite3.connect(path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def execute(self, sql: str, args: list | None = None) -> list[dict]:
        rows = self._conn.execute(sql, args or []).fetchall()
        return [dict(r) for r in rows]

    def executescript(self, script: str) -> None:
        self._conn.executescript(script)

    def commit(self) -> None:
        self._conn.commit()


class TursoBackend(DatabaseBackend):
    """Remote Turso/libSQL HTTP backend."""

    def __init__(self, url: str, auth_token: str):
        from .turso import TursoClient
        self._client = TursoClient(url, auth_token)

    def execute(self, sql: str, args: list | None = None) -> list[dict]:
        return self._client.execute(sql, args)

    def executescript(self, script: str) -> None:
        self._client.executescript(script)


class MemoryDatabase:
    def __init__(self, db_path: str | None = None):
        turso_url = os.environ.get("TURSO_URL", "")
        turso_token = os.environ.get("TURSO_AUTH_TOKEN", "")

        if turso_url and turso_token:
            self._backend = TursoBackend(turso_url, turso_token)
            self.mode = "turso"
            self.db_path = turso_url
        else:
            path = db_path or os.environ.get("DATABASE_PATH", "claude_memory.db")
            if not os.path.isabs(path):
                data_dir = Path.home() / ".claude-memory"
                data_dir.mkdir(exist_ok=True)
                path = str(data_dir / path)
            self._backend = SQLiteBackend(path)
            self.mode = "local"
            self.db_path = path

    def initialize(self) -> None:
        self._backend.executescript(SCHEMA)
        self._backend.commit()

    def read(
        self,
        project: str,
        topic: str | None = None,
        key: str | None = None,
        tags: list[str] | None = None,
    ) -> list[Memory]:
        sql = "SELECT * FROM memories WHERE project = ? AND archived = 0"
        args: list = [project]

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
        rows = self._backend.execute(sql, args)
        return [self._dict_to_memory(r) for r in rows]

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
        tags_json = json.dumps(tags or [])

        rows = self._backend.execute(
            "SELECT id, content FROM memories WHERE project = ? AND topic = ? AND key = ?",
            [project, topic, key],
        )

        if rows:
            memory_id = str(rows[0]["id"])
            old_content = str(rows[0]["content"])
            action = "updated"
            self._backend.execute(
                """UPDATE memories
                   SET content = ?, tags = ?, source = ?, confidence = ?,
                       updated_at = datetime('now'), archived = 0
                   WHERE id = ?""",
                [content, tags_json, source, confidence, memory_id],
            )
        else:
            memory_id = _gen_id()
            old_content = None
            action = "created"
            self._backend.execute(
                """INSERT INTO memories (id, project, topic, key, content, tags, source, confidence)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                [memory_id, project, topic, key, content, tags_json, source, confidence],
            )

        self._backend.execute(
            """INSERT INTO write_log (memory_id, action, instance, old_content, new_content)
               VALUES (?, ?, ?, ?, ?)""",
            [memory_id, "create" if action == "created" else "update", source, old_content, content],
        )

        self._backend.commit()
        return action, memory_id

    def search(self, query: str, project: str | None = None) -> list[Memory]:
        pattern = f"%{query}%"
        sql = "SELECT * FROM memories WHERE archived = 0 AND (content LIKE ? OR key LIKE ? OR topic LIKE ?)"
        args: list = [pattern, pattern, pattern]

        if project:
            sql += " AND project = ?"
            args.append(project)

        sql += " ORDER BY updated_at DESC LIMIT 50"
        rows = self._backend.execute(sql, args)
        return [self._dict_to_memory(r) for r in rows]

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
            args: list = [project, key, limit]
        elif project:
            sql = """SELECT wl.* FROM write_log wl
                     JOIN memories m ON wl.memory_id = m.id
                     WHERE m.project = ?
                     ORDER BY wl.timestamp DESC LIMIT ?"""
            args = [project, limit]
        else:
            sql = "SELECT * FROM write_log ORDER BY timestamp DESC LIMIT ?"
            args = [limit]

        rows = self._backend.execute(sql, args)
        return [
            WriteLogEntry(
                id=int(r["id"]),
                memory_id=str(r["memory_id"]),
                action=str(r["action"]),
                instance=str(r["instance"]),
                timestamp=str(r["timestamp"]),
                old_content=r["old_content"] if r["old_content"] else None,
                new_content=r["new_content"] if r["new_content"] else None,
            )
            for r in rows
        ]

    def archive(self, memory_id: str, source: str) -> bool:
        rows = self._backend.execute(
            "SELECT id, content FROM memories WHERE id = ?", [memory_id]
        )
        if not rows:
            return False

        self._backend.execute(
            "UPDATE memories SET archived = 1, updated_at = datetime('now') WHERE id = ?",
            [memory_id],
        )
        self._backend.execute(
            """INSERT INTO write_log (memory_id, action, instance, old_content, new_content)
               VALUES (?, 'archive', ?, ?, NULL)""",
            [memory_id, source, str(rows[0]["content"])],
        )
        self._backend.commit()
        return True

    def list_memories(self, project: str | None = None) -> list[dict]:
        if project:
            sql = """SELECT project, topic, key, updated_at FROM memories
                     WHERE archived = 0 AND project = ?
                     ORDER BY project, topic, key"""
            rows = self._backend.execute(sql, [project])
        else:
            sql = """SELECT project, topic, key, updated_at FROM memories
                     WHERE archived = 0
                     ORDER BY project, topic, key"""
            rows = self._backend.execute(sql)

        return [
            {
                "project": str(r["project"]),
                "topic": str(r["topic"]),
                "key": str(r["key"]),
                "updated_at": str(r["updated_at"]),
            }
            for r in rows
        ]

    @staticmethod
    def _dict_to_memory(row: dict) -> Memory:
        tags_raw = row.get("tags", "[]")
        if isinstance(tags_raw, str):
            tags = json.loads(tags_raw)
        else:
            tags = tags_raw

        return Memory(
            id=str(row["id"]),
            project=str(row["project"]),
            topic=str(row["topic"]),
            key=str(row["key"]),
            content=str(row["content"]),
            tags=tags,
            source=str(row["source"]),
            confidence=float(row["confidence"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
            supersedes=row.get("supersedes"),
            archived=int(row["archived"]),
        )
