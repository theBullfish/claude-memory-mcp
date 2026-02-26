"""Standalone setup script — initializes the database and prints configuration."""

import json
import sys
from pathlib import Path
from .database import MemoryDatabase


def main():
    db = MemoryDatabase()
    print(f"Initializing database at: {db.db_path}")
    db.initialize()
    print("Schema created successfully.\n")

    # Find the server module path for config
    server_module = Path(__file__).parent / "server.py"
    python_path = sys.executable

    config = {
        "mcpServers": {
            "claude-memory": {
                "command": python_path,
                "args": ["-m", "claude_memory_mcp.server"],
                "env": {
                    "INSTANCE_ID": "claude-code-main",
                },
            }
        }
    }

    print("Add this to your Claude Code settings (~/.claude/settings.json):\n")
    print(json.dumps(config, indent=2))
    print(
        "\nOr for Claude.ai / remote instances, configure the MCP server"
        " to point to the same database path."
    )


if __name__ == "__main__":
    main()
