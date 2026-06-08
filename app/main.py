from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.events import router as events_router
from app.consumers import register_consumers
from app.dependencies import broker, clickhouse_client


register_consumers(broker)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await clickhouse_client.connect()
    await broker.start()

    yield

    await broker.stop()
    await clickhouse_client.disconnect()


app = FastAPI(
    title="Event Service",
    lifespan=lifespan,
)

app.include_router(events_router)
