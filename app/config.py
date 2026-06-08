from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str
    app_host: str
    app_port: int
    app_debug: bool

    kafka_bootstrap_servers: str
    kafka_events_topic: str
    kafka_client_id: str
    kafka_consumer_group: str
    kafka_batch_timeout_ms: int
    kafka_events_retry_topic: str
    kafka_events_dlq_topic: str
    kafka_events_retransmit_topic: str
    kafka_retransmit_consumer_group: str
    max_retry_attempts: int

    retransmit_enabled: bool

    clickhouse_host: str
    clickhouse_port: int
    clickhouse_database: str
    clickhouse_user: str
    clickhouse_password: str
    clickhouse_batch_size: int
    retry_delay_seconds: int

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def kafka_bootstrap_servers_list(self) -> list[str]:
        return [
            server.strip()
            for server in self.kafka_bootstrap_servers.split(",")
            if server.strip()
        ]

    @property
    def clickhouse_url(self) -> str:
        return f"http://{self.clickhouse_host}:{self.clickhouse_port}"


settings = Settings()
