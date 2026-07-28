from __future__ import annotations

import asyncio
import logging
import random
import time

import websockets

from .coinbase import Router, subscribe_message
from .config import Config
from .sink import Sink

log = logging.getLogger("producer.runner")


class Backoff:
    def __init__(self, base_s: float, cap_s: float):
        self.base_s = base_s
        self.cap_s = cap_s
        self.attempt = 0

    def next(self) -> float:
        delay = min(self.cap_s, self.base_s * (2**self.attempt))
        self.attempt += 1
        return delay * random.uniform(0.5, 1.5)

    def reset(self) -> None:
        self.attempt = 0


async def run(
    cfg: Config,
    sink: Sink,
    router: Router | None = None,
    max_epochs: int | None = None,
) -> None:
    """Connect, subscribe, route messages; reconnect forever with jittered
    exponential backoff. Each (re)connection increments conn_epoch.

    max_epochs limits the number of connection attempts (tests only).
    """
    router = router or Router(cfg, sink)
    backoff = Backoff(cfg.backoff_base_s, cfg.backoff_cap_s)
    epoch = 0
    while max_epochs is None or epoch < max_epochs:
        epoch += 1
        try:
            async with websockets.connect(cfg.ws_url, max_size=2**23, open_timeout=10) as ws:
                log.info("ws.connected", extra={"ctx": {"epoch": epoch, "url": cfg.ws_url}})
                await ws.send(subscribe_message(cfg.products))
                connected_at = time.monotonic()
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=cfg.idle_timeout_s)
                    await router.route(raw, epoch)
                    if backoff.attempt and time.monotonic() - connected_at > cfg.stable_after_s:
                        backoff.reset()
        except TimeoutError:
            log.warning(
                "ws.idle_timeout",
                extra={"ctx": {"epoch": epoch, "idle_s": cfg.idle_timeout_s}},
            )
        except (websockets.WebSocketException, OSError) as e:
            log.warning(
                "ws.disconnected",
                extra={"ctx": {"epoch": epoch, "error": f"{type(e).__name__}: {e}"}},
            )
        if max_epochs is not None and epoch >= max_epochs:
            return
        router.counts["reconnects"] += 1
        delay = backoff.next()
        log.info("ws.reconnecting", extra={"ctx": {"epoch": epoch, "delay_s": round(delay, 2)}})
        await asyncio.sleep(delay)
