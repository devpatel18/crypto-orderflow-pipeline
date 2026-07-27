import asyncio
import contextlib
import logging
import signal

from .coinbase import Router
from .config import Config
from .log import setup
from .runner import run
from .sink import KafkaSink

log = logging.getLogger("producer.main")


async def _log_stats(router: Router, interval_s: float = 60.0) -> None:
    while True:
        await asyncio.sleep(interval_s)
        log.info("producer.stats", extra={"ctx": dict(router.counts)})


async def main() -> None:
    setup()
    cfg = Config.from_env()
    sink = KafkaSink(cfg.bootstrap, dlq_topic=cfg.dlq_topic)
    await sink.start()
    router = Router(cfg, sink)
    run_task = asyncio.create_task(run(cfg, sink, router))
    stats_task = asyncio.create_task(_log_stats(router))

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, run_task.cancel)

    try:
        await run_task
    except asyncio.CancelledError:
        log.info("producer.shutdown", extra={"ctx": dict(router.counts)})
    finally:
        stats_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await stats_task
        await sink.stop()


if __name__ == "__main__":
    asyncio.run(main())
