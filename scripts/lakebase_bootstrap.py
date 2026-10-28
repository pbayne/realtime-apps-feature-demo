"""Apply the sidekick schema to a Lakebase Autoscaling endpoint as the current CLI user.

For dev databases outside the bundle flow (the bundle's setup job runs init.py instead).
TABLE_DDL is read from src/notebooks/init.py so there is one source of truth.

  .venv/bin/python scripts/lakebase_bootstrap.py --profile e2-demo \
      --endpoint projects/call-sidekick-pbayne/branches/production/endpoints/primary
"""

from __future__ import annotations

import argparse
import ast
import asyncio
from pathlib import Path

import asyncpg
from databricks.sdk import WorkspaceClient

INIT = Path(__file__).resolve().parents[1] / "src" / "notebooks" / "init.py"


def table_ddl() -> list[str]:
    for node in ast.parse(INIT.read_text(encoding="utf-8")).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "TABLE_DDL" for t in node.targets):
            return ast.literal_eval(node.value)
    raise SystemExit(f"TABLE_DDL not found in {INIT}")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--profile", required=True)
    ap.add_argument("--endpoint", required=True, help="projects/<p>/branches/<b>/endpoints/<e>")
    ap.add_argument("--database", default="databricks_postgres")
    args = ap.parse_args()

    w = WorkspaceClient(profile=args.profile)
    ep = w.postgres.get_endpoint(args.endpoint)
    host = ep.status.hosts.host
    user = w.current_user.me().user_name
    token = w.postgres.generate_database_credential(endpoint=args.endpoint).token

    conn = await asyncpg.connect(host=host, port=5432, user=user, password=token,
                                 database=args.database, ssl="require")
    try:
        for stmt in table_ddl():
            await conn.execute(stmt)
        tables = await conn.fetch(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY 1")
        triggers = await conn.fetch(
            "SELECT tgname FROM pg_trigger WHERE NOT tgisinternal ORDER BY 1")
    finally:
        await conn.close()
    print(f"host={host} user={user} db={args.database}")
    print("tables:", [r["table_name"] for r in tables])
    print("triggers:", [r["tgname"] for r in triggers])


if __name__ == "__main__":
    asyncio.run(main())
