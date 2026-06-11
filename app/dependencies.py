from faststream.kafka import KafkaBroker

from app.config import settings
from app.messaging.publisher import EventPublisher
from app.messaging.retransmitter import EventRetransmitter
from app.storage.clickhouse.client import ClickHouseClient


clickhouse_client = ClickHouseClient()

broker = KafkaBroker(
    bootstrap_servers=settings.kafka_bootstrap_servers_list,
    client_id=settings.kafka_client_id
)

event_retransmitter = EventRetransmitter(
    broker=broker,
    topic=settings.kafka_events_retransmit_topic,
)

event_publisher = EventPublisher(
    broker=broker,
    events_topic=settings.kafka_events_topic,
    retry_topic=settings.kafka_events_retry_topic,
    dlq_topic=settings.kafka_events_dlq_topic,
)


def get_event_publisher() -> EventPublisher:
    return event_publisher


def get_clickhouse_client() -> ClickHouseClient:
    return clickhouse_client
