import argparse
import asyncio
import random
from datetime import datetime, timezone
from uuid import uuid4

from aiohttp import ClientSession, ClientTimeout


EVENT_TYPES = [
    "user_click",
    "page_view",
    "payment_success",
    "order_created",
    "product_view",
]

SOURCES = [
    "web",
    "mobile",
    "backend",
]


def build_test_event(number: int, run_id: str) -> dict:
    event_type = random.choice(EVENT_TYPES)
    source = random.choice(SOURCES)

    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "source": source,
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S"),
        "payload": {
            "test_run": run_id,
            "number": number,
            "page": random.choice(
                [
                    "/catalog",
                    "/product/1",
                    "/cart",
                    "/profile",
                    "/checkout",
                ]
            ),
            "button": random.choice(
                [
                    "buy",
                    "details",
                    "add_to_cart",
                    "login",
                    "pay",
                ]
            ),
        },
    }


async def send_event(
    session: ClientSession,
    url: str,
    event: dict,
    number: int,
) -> None:
    async with session.post(url, json=event) as response:
        response_text = await response.text()

        if response.status != 202:
            raise RuntimeError(
                f"Failed to send event {number}. "
                f"Status: {response.status}. "
                f"Response: {response_text}"
            )

        print(
            f"Sent event {number}: "
            f"event_id={event['event_id']}, "
            f"event_type={event['event_type']}, "
            f"source={event['source']}"
        )


async def produce_test_events(
    base_url: str,
    count: int,
    delay: float,
    concurrency: int,
) -> None:
    run_id = str(uuid4())
    url = base_url.rstrip("/") + "/events"

    print("Start producing test events through API")
    print(f"base_url={base_url}")
    print(f"url={url}")
    print(f"run_id={run_id}")
    print(f"count={count}")
    print(f"delay={delay}")
    print(f"concurrency={concurrency}")

    timeout = ClientTimeout(total=30)
    semaphore = asyncio.Semaphore(concurrency)

    async with ClientSession(timeout=timeout) as session:

        async def send_with_limit(number: int) -> None:
            async with semaphore:
                event = build_test_event(
                    number=number,
                    run_id=run_id,
                )

                await send_event(
                    session=session,
                    url=url,
                    event=event,
                    number=number,
                )

                if delay > 0:
                    await asyncio.sleep(delay)

        tasks = [
            asyncio.create_task(send_with_limit(number))
            for number in range(1, count + 1)
        ]

        await asyncio.gather(*tasks)

    print("Test events producing completed")
    print(f"Check ClickHouse by test_run={run_id}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce test events through Event Service API"
    )

    parser.add_argument(
        "--base-url",
        type=str,
        default="http://127.0.0.1:8010",
        help="Base URL of Event Service API",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of test events to produce",
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0,
        help="Delay between requests in seconds",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Number of concurrent HTTP requests",
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    asyncio.run(
        produce_test_events(
            base_url=args.base_url,
            count=args.count,
            delay=args.delay,
            concurrency=args.concurrency,
        )
    )
