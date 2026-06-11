from faststream.kafka import KafkaBroker

from app.schemas.events import EventDLQMessageSchema, EventInSchema, EventRetryMessageSchema


class EventPublisher:
    def __init__(
        self,
        broker: KafkaBroker,
        events_topic: str,
        retry_topic: str,
        dlq_topic: str,
    ) -> None:
        self.broker = broker
        self.events_topic = events_topic
        self.retry_topic = retry_topic
        self.dlq_topic = dlq_topic

    async def publish_event(self, event: EventInSchema) -> None:
        await self.broker.publish(
            message=event.model_dump(mode="json"),
            topic=self.events_topic,
        )

    async def publish_retry_event(
        self,
        event: EventInSchema,
        retry_count: int,
        error: str,
    ) -> None:
        retry_message = EventRetryMessageSchema(
            event=event,
            retry_count=retry_count,
            error=self._short_error(error),
        )

        await self.broker.publish(
            message=retry_message.model_dump(mode="json"),
            topic=self.retry_topic,
        )

    async def publish_retry_events(
        self,
        events: list[EventInSchema],
        retry_count: int,
        error: str,
    ) -> None:
        for event in events:
            await self.publish_retry_event(
                event=event,
                retry_count=retry_count,
                error=error,
            )

    async def publish_dlq_event(
        self,
        event: EventInSchema,
        retry_count: int,
        error: str,
    ) -> None:
        dlq_message = EventDLQMessageSchema(
            event=event,
            retry_count=retry_count,
            error=self._short_error(error),
        )

        await self.broker.publish(
            message=dlq_message.model_dump(mode="json"),
            topic=self.dlq_topic,
        )

    async def publish_dlq_events(
        self,
        messages: list[EventRetryMessageSchema],
        error: str,
    ) -> None:
        for message in messages:
            await self.publish_dlq_event(
                event=message.event,
                retry_count=message.retry_count,
                error=error,
            )

    @staticmethod
    def _short_error(error: str, limit: int = 1000) -> str:
        return error[:limit]
