from faststream.kafka import KafkaBroker

from app.schemas.events import EventInSchema


class EventRetransmitter:
    def __init__(
        self,
        broker: KafkaBroker,
        topic: str,
    ) -> None:
        self.broker = broker
        self.topic = topic

    async def retransmit_event(self, event: EventInSchema) -> None:
        await self.broker.publish(
            message=event.model_dump(mode="json"),
            topic=self.topic,
            headers={
                "retransmitted": "true",
                "event_type": event.event_type,
                "source": event.source,
            },
        )

    async def retransmit_events(self, events: list[EventInSchema]) -> None:
        for event in events:
            await self.retransmit_event(event)
