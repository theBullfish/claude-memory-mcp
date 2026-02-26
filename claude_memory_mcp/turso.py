"""Turso HTTP API client — pure Python, no native deps."""

import json
import urllib.request
import urllib.error


class TursoClient:
    """Minimal client for the Turso/libSQL HTTP API (v2 pipeline)."""

    def __init__(self, url: str, auth_token: str):
        # Convert libsql:// to https://
        self.base_url = url.replace("libsql://", "https://")
        self.auth_token = auth_token
        self.pipeline_url = f"{self.base_url}/v2/pipeline"

    def execute(self, sql: str, args: list | None = None) -> list[dict]:
        """Execute a single SQL statement. Returns list of row dicts."""
        stmt: dict = {"sql": sql}
        if args:
            stmt["args"] = [self._encode_arg(a) for a in args]

        payload = {
            "requests": [
                {"type": "execute", "stmt": stmt},
                {"type": "close"},
            ]
        }

        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.pipeline_url,
            data=data,
            headers={
                "Authorization": f"Bearer {self.auth_token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Turso HTTP {e.code}: {body}") from e

        # Parse the pipeline response
        results = result.get("results", [])
        if not results:
            return []

        first = results[0]
        if first.get("type") == "error":
            error = first.get("error", {})
            raise RuntimeError(f"Turso SQL error: {error.get('message', str(error))}")

        response = first.get("response", {})
        resp_result = response.get("result", {})
        cols = [c["name"] for c in resp_result.get("cols", [])]
        rows_raw = resp_result.get("rows", [])

        rows = []
        for row_data in rows_raw:
            row = {}
            for i, col in enumerate(cols):
                cell = row_data[i]
                row[col] = cell.get("value") if isinstance(cell, dict) else cell
            rows.append(row)

        return rows

    def executescript(self, script: str) -> None:
        """Execute multiple SQL statements separated by semicolons."""
        statements = [s.strip() for s in script.split(";") if s.strip()]
        requests = []
        for stmt in statements:
            requests.append({"type": "execute", "stmt": {"sql": stmt}})
        requests.append({"type": "close"})

        payload = {"requests": requests}
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self.pipeline_url,
            data=data,
            headers={
                "Authorization": f"Bearer {self.auth_token}",
                "Content-Type": "application/json",
            },
        )

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                result = json.loads(resp.read())
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Turso HTTP {e.code}: {body}") from e

        # Check for errors
        for r in result.get("results", []):
            if r.get("type") == "error":
                error = r.get("error", {})
                raise RuntimeError(f"Turso SQL error: {error.get('message', str(error))}")

    @staticmethod
    def _encode_arg(value) -> dict:
        if value is None:
            return {"type": "null", "value": None}
        elif isinstance(value, int):
            return {"type": "integer", "value": str(value)}
        elif isinstance(value, float):
            return {"type": "float", "value": value}
        elif isinstance(value, str):
            return {"type": "text", "value": value}
        else:
            return {"type": "text", "value": str(value)}
