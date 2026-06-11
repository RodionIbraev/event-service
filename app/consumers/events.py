import asyncio

from faststream import Logger
from faststream.kafka import KafkaRouter

from app.config import settings
from app.dependencies import clickhouse_client, event_publisher
from app.schemas.events import EventInSchema, EventRetryMessageSchema


events_consumer_router = KafkaRouter()


@events_consumer_router.subscriber(
    settings.kafka_events_topic,
    group_id=settings.kafka_consumer_group,
    auto_offset_reset="latest",
    batch=True,
    max_records=settings.clickhouse_batch_size,
    batch_timeout_ms=settings.kafka_batch_timeout_ms,
)
async def handle_events(events: list[EventInSchema], logger: Logger) -> None:
    logger.info(f"Events batch received from Kafka: {len(events)}")

    try:
        await clickhouse_client.save_events(events)
    except Exception as exc:
        error = str(exc)

        logger.error(
            f"Failed to save events batch to ClickHouse. "
            f"Batch size: {len(events)}. Error: {error}"
        )

        await event_publisher.publish_retry_events(
            events=events,
            retry_count=1,
            error=error,
        )

        logger.info(f"Events batch sent to retry topic: {len(events)}")
        return

    logger.info(f"Events batch saved to ClickHouse: {len(events)}")


@events_consumer_router.subscriber(
    settings.kafka_events_retry_topic,
    group_id=f"{settings.kafka_consumer_group}-retry",
    auto_offset_reset="latest",
    batch=True,
    max_records=settings.clickhouse_batch_size,
    batch_timeout_ms=settings.kafka_batch_timeout_ms,
)
async def handle_retry_events(
    retry_messages: list[EventRetryMessageSchema],
    logger: Logger,
) -> None:
    logger.info(f"Retry batch received from Kafka: {len(retry_messages)}")

    if settings.retry_delay_seconds > 0:
        await asyncio.sleep(settings.retry_delay_seconds)

    events = [
        retry_message.event
        for retry_message in retry_messages
    ]

    try:
        await clickhouse_client.save_events(events)
    except Exception as exc:
        error = str(exc)

        logger.error(
            f"Failed to save retry batch to ClickHouse. "
            f"Batch size: {len(retry_messages)}. Error: {error}"
        )

        retry_again_count = 0
        dlq_count = 0

        for retry_message in retry_messages:
            next_retry_count = retry_message.retry_count + 1

            if next_retry_count >= settings.max_retry_attempts:
                await event_publisher.publish_dlq_event(
                    event=retry_message.event,
                    retry_count=next_retry_count,
                    error=error,
                )
                dlq_count += 1
            else:
                await event_publisher.publish_retry_event(
                    event=retry_message.event,
                    retry_count=next_retry_count,
                    error=error,
                )
                retry_again_count += 1

        logger.info(
            f"Retry batch processed after failure. "
            f"Sent to retry again: {retry_again_count}. "
            f"Sent to DLQ: {dlq_count}"
        )
        return

    logger.info(f"Retry batch saved to ClickHouse: {len(retry_messages)}")
