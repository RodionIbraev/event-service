from faststream.kafka import KafkaBroker

from app.consumers.events import events_consumer_router
from app.consumers.retransmit import retransmit_consumer_router


def register_consumers(broker: KafkaBroker) -> None:
    broker.include_router(events_consumer_router)
    broker.include_router(retransmit_consumer_router)
