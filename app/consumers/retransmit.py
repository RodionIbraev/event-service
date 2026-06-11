from faststream import Logger
from faststream.kafka import KafkaRouter

from app.config import settings
from app.dependencies import event_retransmitter
from app.schemas.events import EventInSchema


retransmit_consumer_router = KafkaRouter()


@retransmit_consumer_router.subscriber(
    settings.kafka_events_topic,
    group_id=settings.kafka_retransmit_consumer_group,
    auto_offset_reset="latest",
    batch=True,
    max_records=settings.clickhouse_batch_size,
    batch_timeout_ms=settings.kafka_batch_timeout_ms,
)
async def retransmit_events(events: list[EventInSchema], logger: Logger) -> None:
    if not settings.retransmit_enabled:
        logger.info("Retransmission is disabled")
        return

    logger.info(f"Retransmit batch received from Kafka: {len(events)}")

    await event_retransmitter.retransmit_events(events)

    logger.info(f"Retransmit batch sent to external topic: {len(events)}")
