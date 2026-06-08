import json
from datetime import datetime, timezone

from aiochclient import ChClient
from aiohttp import ClientSession

from app.config import settings
from app.schemas.events import EventIn, EventOut, EventStatsOut


class ClickHouseClient:
    def __init__(self) -> None:
        self.session: ClientSession | None = None
        self.client: ChClient | None = None

    async def connect(self) -> None:
        self.session = ClientSession()

        self.client = ChClient(
            self.session,
            url=settings.clickhouse_url,
            user=settings.clickhouse_user,
            password=settings.clickhouse_password,
            database=settings.clickhouse_database,
        )

    async def disconnect(self) -> None:
        if self.session is not None:
            await self.session.close()

    async def save_events(self, events: list[EventIn]) -> None:
        if self.client is None:
            raise RuntimeError("ClickHouse client is not connected")

        if not events:
            return

        rows = [
            (
                str(event.event_id),
                event.event_type,
                event.source,
                self._normalize_datetime(event.event_time),
                json.dumps(event.payload, ensure_ascii=False),
            )
            for event in events
        ]

        await self.client.execute(
            """
            INSERT INTO events
            (
                event_id,
                event_type,
                source,
                event_time,
                payload
            )
            VALUES
            """,
            *rows,
        )

    async def get_events(
            self,
            event_type: str | None = None,
            source: str | None = None,
            date_from: datetime | None = None,
            date_to: datetime | None = None,
            limit: int = 100,
    ) -> list[EventOut]:
        if self.client is None:
            raise RuntimeError("ClickHouse client is not connected")

        where_parts: list[str] = []

        if event_type:
            where_parts.append(f"event_type = {self._quote(event_type)}")

        if source:
            where_parts.append(f"source = {self._quote(source)}")

        if date_from:
            normalized_date_from = self._normalize_datetime(date_from)
            where_parts.append(
                f"event_time >= toDateTime({self._quote_datetime(normalized_date_from)})"
            )

        if date_to:
            normalized_date_to = self._normalize_datetime(date_to)
            where_parts.append(
                f"event_time < toDateTime({self._quote_datetime(normalized_date_to)})"
            )

        where_sql = ""

        if where_parts:
            where_sql = "WHERE " + " AND ".join(where_parts)

        query = f"""
            SELECT
                event_id,
                event_type,
                source,
                event_time,
                payload,
                created_at
            FROM events
            {where_sql}
            ORDER BY created_at DESC
            LIMIT {limit}
        """

        rows = await self.client.fetch(query)

        return [
            EventOut(
                event_id=row["event_id"],
                event_type=row["event_type"],
                source=row["source"],
                event_time=row["event_time"],
                payload=json.loads(row["payload"]),
                created_at=row["created_at"],
            )
            for row in rows
        ]

    async def get_events_stats(
            self,
            source: str | None = None,
            date_from: datetime | None = None,
            date_to: datetime | None = None,
    ) -> list[EventStatsOut]:
        if self.client is None:
            raise RuntimeError("ClickHouse client is not connected")

        where_parts: list[str] = []

        if source:
            where_parts.append(f"source = {self._quote(source)}")

        if date_from:
            normalized_date_from = self._normalize_datetime(date_from)
            where_parts.append(
                f"event_time >= toDateTime({self._quote_datetime(normalized_date_from)})"
            )

        if date_to:
            normalized_date_to = self._normalize_datetime(date_to)
            where_parts.append(
                f"event_time < toDateTime({self._quote_datetime(normalized_date_to)})"
            )

        where_sql = ""

        if where_parts:
            where_sql = "WHERE " + " AND ".join(where_parts)

        query = f"""
            SELECT
                event_type,
                count() AS count
            FROM events
            {where_sql}
            GROUP BY event_type
            ORDER BY count DESC
        """

        rows = await self.client.fetch(query)

        return [
            EventStatsOut(
                event_type=row["event_type"],
                count=row["count"],
            )
            for row in rows
        ]

    @staticmethod
    def _normalize_datetime(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value

        return value.astimezone(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _quote(value: str) -> str:
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"

    @staticmethod
    def _quote_datetime(value: datetime) -> str:
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"