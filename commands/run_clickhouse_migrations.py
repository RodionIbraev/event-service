import asyncio
import hashlib
from pathlib import Path

from aiochclient import ChClient
from aiohttp import ClientSession

from app.config import settings


BASE_DIR = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = BASE_DIR/"migrations"/"clickhouse"


def get_sql_queries(sql: str) -> list[str]:
    queries: list[str] = []

    for query in sql.split(";"):
        query = query.strip()

        if query:
            queries.append(query)

    return queries


def get_checksum(sql: str) -> str:
    return hashlib.sha256(sql.encode("utf-8")).hexdigest()


async def create_database(client: ChClient) -> None:
    await client.execute(
        f"CREATE DATABASE IF NOT EXISTS {settings.clickhouse_database}"
    )


async def create_migrations_table(client: ChClient) -> None:
    await client.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {settings.clickhouse_database}.schema_migrations
        (
            filename String,
            checksum String,
            applied_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree
        ORDER BY filename
        """
    )


async def get_applied_migrations(client: ChClient) -> dict[str, str]:
    rows = await client.fetch(
        f"""
        SELECT filename, checksum
        FROM {settings.clickhouse_database}.schema_migrations
        """
    )

    return {
        row["filename"]: row["checksum"]
        for row in rows
    }


async def save_applied_migration(
    client: ChClient,
    filename: str,
    checksum: str,
) -> None:
    await client.execute(
        f"""
        INSERT INTO {settings.clickhouse_database}.schema_migrations
        (
            filename,
            checksum
        )
        VALUES
        """,
        (filename, checksum),
    )


async def apply_migration(
    client: ChClient,
    migration_path: Path,
    applied_migrations: dict[str, str],
) -> None:
    filename = migration_path.name
    sql = migration_path.read_text(encoding="utf-8")
    checksum = get_checksum(sql)

    if filename in applied_migrations:
        old_checksum = applied_migrations[filename]

        if old_checksum != checksum:
            raise RuntimeError(
                f"Migration {filename} was already applied, "
                f"but its checksum has changed"
            )

        print(f"Skip migration: {filename}")
        return

    print(f"Apply migration: {filename}")

    for query in get_sql_queries(sql):
        await client.execute(query)

    await save_applied_migration(
        client=client,
        filename=filename,
        checksum=checksum,
    )

    print(f"Migration applied: {filename}")


async def run_migrations() -> None:
    if not MIGRATIONS_DIR.exists():
        raise RuntimeError(f"Migrations directory not found: {MIGRATIONS_DIR}")

    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))

    if not migration_files:
        print("No ClickHouse migrations found")
        return

    async with ClientSession() as session:
        client = ChClient(
            session,
            url=settings.clickhouse_url,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database="default",
        )

        alive = await client.is_alive()

        if not alive:
            raise RuntimeError("ClickHouse is not available")

        await create_database(client)
        await create_migrations_table(client)

        applied_migrations = await get_applied_migrations(client)

        for migration_path in migration_files:
            await apply_migration(
                client=client,
                migration_path=migration_path,
                applied_migrations=applied_migrations,
            )

    print("ClickHouse migrations completed")


if __name__ == "__main__":
    asyncio.run(run_migrations())
