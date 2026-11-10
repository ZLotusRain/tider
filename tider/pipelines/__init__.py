import time
from threading import Lock
from operator import itemgetter
from typing import Callable, Deque, Dict
from collections import defaultdict, deque

from tider import Item, signals
from tider.utils.log import get_logger
from tider.utils.imports import symbol_by_name
from tider.utils.misc import build_from_crawler

logger = get_logger(__name__)


class LogPipeline:

    def process_item(self, item):
        data = item
        if isinstance(item, Item):
            data = item.jsonify()
        logger.info(f'Obtained item: {data}')


class ItemPipelineManager:
    """item_batch_failed observers run synchronously before ownership is released.
    Receivers own durable recovery, and processing continues after notification.
    """

    def __init__(self, *pipelines, crawler=None, max_items=1, max_wait=1.0):
        self.max_items = max_items
        self.pipelines = pipelines or (LogPipeline(),)
        self.methods: Dict[str, Deque[Callable]] = defaultdict(deque)
        for pipeline in self.pipelines:
            self._add_pipeline(pipeline)

        self.crawler = crawler
        self.max_wait = max_wait
        self._batch_started = None
        self.items = deque()
        self.processing = []
        self.running = False
        # Protect admission, completion and lifecycle ownership.
        self._lock = Lock()
        self._close_lock = Lock()
        self._worker_running = False
        # Include queued, buffered and executing items until completion.
        self._pending = 0
        self._closing = False

    @classmethod
    def from_settings(cls, settings, crawler=None):
        max_wait = settings.getfloat('ITEM_PROCESSOR_MAX_WAIT', 1.0)
        pipelines = []
        pipeline_setting = settings.get("ITEM_PIPELINES")
        cls_paths = [k for k, v in sorted(pipeline_setting.items(), key=itemgetter(1))]
        for cls_path in cls_paths:
            pipeline_cls = symbol_by_name(cls_path)
            pipeline = build_from_crawler(pipeline_cls, crawler)
            pipelines.append(pipeline)
        return cls(*pipelines, crawler=crawler, max_items=settings.getint('ITEM_PROCESSOR_MAX_ITEMS', 1), max_wait=max_wait)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings, crawler)

    def _add_pipeline(self, pipeline):
        if hasattr(pipeline, 'close'):
            self.methods['close'].appendleft(pipeline.close)
        if self.max_items > 1 and hasattr(pipeline, "process_items"):
            self.methods["process_items"].append(pipeline.process_items)
        elif hasattr(pipeline, "process_item"):
            self.methods["process_item"].append(pipeline.process_item)
        elif hasattr(pipeline, "process_items"):
            self.methods["process_items"].append(pipeline.process_items)

    def active(self):
        return bool(self._pending)

    def needs_backout(self):
        return self._pending >= self.max_items * 2

    def _release(self, count):
        with self._lock:
            self._pending -= count

    def _flush_items(self):
        if not self.processing:
            return
        items, self.processing = self.processing, []
        self._batch_started = None
        count = len(items)
        try:
            self.process_items(items)
        except Exception as exc:
            # Publication may have partially succeeded. Never replay this batch.
            logger.exception('Error processing batch of %s item(s)', count)
            signals.item_batch_failed.send(sender=self, crawler=self.crawler, items=items, exception=exc)
        finally:
            self._release(count)

    def process_queue(self):
        with self._lock:
            if self._closing or self._worker_running:
                return
            self.running = self._worker_running = True
        try:
            self._process_queue()
        finally:
            self.running = False
            self._worker_running = False

    def _process_queue(self):
        last_pause = time.monotonic()
        while self.running or self.items:
            try:
                item = self.items.pop()
            except IndexError:
                pass
            else:
                buffered = False
                try:
                    self.process_item(item)
                    if not self.processing:
                        self._batch_started = time.monotonic()
                    self.processing.append(item)
                    buffered = True
                except Exception:
                    logger.exception('Error processing item')
                finally:
                    if not buffered:
                        self._release(1)
                del item
            if self.processing and (
                len(self.processing) >= self.max_items
                or time.monotonic() - self._batch_started >= self.max_wait
            ):
                self._flush_items()
            # Keep idle backoff and a positive pause after each busy time slice.
            now = time.monotonic()
            pause = 0.001 if not self.items or now - last_pause >= 0.002 else 0
            self.crawler.sleep(pause)
            if pause:
                last_pause = time.monotonic()
        self._flush_items()

    def process_item_future(self, item):
        while True:
            with self._lock:
                if self._closing:
                    raise RuntimeError('Item processor is closing')
                # Preserve pre-start enqueueing; the running worker bounds accepted work.
                if not self._worker_running or self._pending < self.max_items * 2:
                    self._pending += 1
                    try:
                        self.items.append(item)
                    except BaseException:
                        self._pending -= 1
                        raise
                    return
            self.crawler.sleep(0.001)

    def process_item(self, item):
        for method in self.methods.get("process_item", []):
            method(item)

    def process_items(self, items):
        for method in self.methods.get("process_items", []):
            method(items)

    def close(self):
        with self._close_lock:
            with self._lock:
                if self._closing:
                    return
                self._closing = True
                self.running = False
            try:
                while self._worker_running:
                    time.sleep(0.001)
                self._process_queue()
                error = None
                for close_func in self.methods.get('close', []):
                    try:
                        close_func()
                    except Exception as exc:
                        if error is None:
                            error = exc
                if error is not None:
                    raise error
            finally:
                self.methods.clear()
