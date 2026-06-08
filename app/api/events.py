from datetime import datetime

from fastapi import APIRouter, Depends, Query, status

from app.dependencies import get_event_publisher, get_clickhouse_client
from app.messaging.publisher import EventPublisher
from app.schemas.events import EventIn, EventAccepted, EventOut, EventStatsOut
from app.storage.clickhouse.client import ClickHouseClient

router = APIRouter(prefix="/events", tags=["events"])


@router.post(
    "",
    response_model=EventAccepted,
    status_code=status.HTTP_202_ACCEPTED,
)
async def create_event(
    event: EventIn,
    publisher: EventPublisher = Depends(get_event_publisher),
) -> EventAccepted:
    await publisher.publish_event(event)

    return EventAccepted(
        status="accepted",
        event_id=event.event_id,
    )


@router.get("", response_model=list[EventOut])
async def get_events(
    event_type: str | None = None,
    source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=1000),
    clickhouse_client: ClickHouseClient = Depends(get_clickhouse_client),
) -> list[EventOut]:

    return await clickhouse_client.get_events(
        event_type=event_type,
        source=source,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
    )


@router.get("/stats", response_model=list[EventStatsOut])
async def get_events_stats(
    source: str | None = None,
    date_from: datetime | None = None,
    date_to: datetime | None = None,
    clickhouse_client: ClickHouseClient = Depends(get_clickhouse_client),
) -> list[EventStatsOut]:

    return await clickhouse_client.get_events_stats(
        source=source,
        date_from=date_from,
        date_to=date_to,
    )
