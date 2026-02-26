"""MCP server — exposes Claude memory as 6 tools over stdio."""

import os
import sys
from mcp.server.fastmcp import FastMCP
from .database import MemoryDatabase

INSTANCE_ID = os.environ.get("INSTANCE_ID", "claude-unknown")

db = MemoryDatabase()
mcp = FastMCP("claude-memory")


@mcp.tool()
def memory_read(
    project: str,
    topic: str | None = None,
    key: str | None = None,
    tags: list[str] | None = None,
) -> str:
    """Read memories from the database. Returns all matching memories for a
    project, optionally filtered by topic, key, or tags."""
    memories = db.read(project, topic, key, tags)

    if not memories:
        parts = [f'project="{project}"']
        if topic:
            parts.append(f'topic="{topic}"')
        if key:
            parts.append(f'key="{key}"')
        return f"No memories found for {', '.join(parts)}."

    lines = []
    for m in memories:
        tags_str = ", ".join(m.tags) if m.tags else "none"
        lines.append(
            f"[{m.project}/{m.topic}/{m.key}] (id: {m.id}, confidence: {m.confidence})\n"
            f"  Updated: {m.updated_at} by {m.source}\n"
            f"  Tags: {tags_str}\n"
            f"  Content: {m.content}"
        )
    return "\n\n".join(lines)


@mcp.tool()
def memory_write(
    project: str,
    topic: str,
    key: str,
    content: str,
    tags: list[str] | None = None,
    confidence: float = 1.0,
) -> str:
    """Write or update a memory. Uses upsert — creates if new, updates if
    project/topic/key already exists. All writes are logged with full history."""
    action, memory_id = db.write(project, topic, key, content, INSTANCE_ID, tags, confidence)
    return f"Memory {action}. ID: {memory_id}. Key: {project}/{topic}/{key}. Logged by {INSTANCE_ID}."


@mcp.tool()
def memory_search(query: str, project: str | None = None) -> str:
    """Full-text search across all memories. Searches content, key, and topic fields."""
    memories = db.search(query, project)

    if not memories:
        scope = f' in project "{project}"' if project else ""
        return f'No memories matching "{query}"{scope}.'

    lines = []
    for m in memories:
        preview = m.content[:200] + ("..." if len(m.content) > 200 else "")
        lines.append(f"[{m.project}/{m.topic}/{m.key}] (id: {m.id})\n  {preview}")

    return f"Found {len(memories)} result(s):\n\n" + "\n\n".join(lines)


@mcp.tool()
def memory_history(
    project: str | None = None,
    key: str | None = None,
    limit: int = 50,
) -> str:
    """View the write log — who changed what, when. Shows diffs for updates."""
    entries = db.history(project, key, limit)

    if not entries:
        return "No history entries found."

    lines = []
    for e in entries:
        line = f"[{e.timestamp}] {e.action} by {e.instance} (memory: {e.memory_id})"
        if e.action == "update" and e.old_content:
            old_preview = e.old_content[:100] + ("..." if len(e.old_content) > 100 else "")
            new_preview = (e.new_content or "")[:100] + ("..." if len(e.new_content or "") > 100 else "")
            line += f"\n  Was: {old_preview}\n  Now: {new_preview}"
        elif e.action == "create":
            preview = (e.new_content or "")[:100] + ("..." if len(e.new_content or "") > 100 else "")
            line += f"\n  Content: {preview}"
        elif e.action == "archive":
            preview = (e.old_content or "")[:100] + ("..." if len(e.old_content or "") > 100 else "")
            line += f"\n  Archived: {preview}"
        lines.append(line)

    return "\n\n".join(lines)


@mcp.tool()
def memory_archive(id: str) -> str:
    """Soft-delete a memory by ID. Never hard-deletes — the memory and its
    history are preserved and can be recovered."""
    success = db.archive(id, INSTANCE_ID)
    if success:
        return f"Memory {id} archived by {INSTANCE_ID}."
    return f"Memory {id} not found."


@mcp.tool()
def memory_list(project: str | None = None) -> str:
    """List all active memories — shows project/topic/key structure without
    full content. Use this to discover what's in the database."""
    entries = db.list_memories(project)

    if not entries:
        if project:
            return f'No memories in project "{project}".'
        return "Memory database is empty."

    # Group by project then topic
    grouped: dict[str, dict[str, list[str]]] = {}
    for e in entries:
        proj = e["project"]
        top = e["topic"]
        if proj not in grouped:
            grouped[proj] = {}
        if top not in grouped[proj]:
            grouped[proj][top] = []
        grouped[proj][top].append(f"{e['key']} ({e['updated_at']})")

    lines = [f"{len(entries)} active memories:\n"]
    for proj, topics in grouped.items():
        lines.append(f"{proj}/")
        for top, keys in topics.items():
            lines.append(f"  {top}/")
            for k in keys:
                lines.append(f"    {k}")

    return "\n".join(lines)


def main():
    db.initialize()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
