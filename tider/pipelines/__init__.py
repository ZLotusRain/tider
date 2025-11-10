import time
from operator import itemgetter
from typing import Callable, Deque, Dict
from collections import defaultdict, deque

from tider import Item
from tider.utils.log import get_logger
from tider.utils.misc import build_from_crawler, symbol_by_name

logger = get_logger(__name__)


class LogPipeline:

    def process_item(self, item):
        data = item
        if isinstance(item, Item):
            data = item.jsonify()
        logger.info(f'Obtained item: {data}')


class ItemPipelineManager:
    def __init__(self, *pipelines, crawler=None, max_items=1):
        pipelines = pipelines if len(pipelines) > 0 else (LogPipeline(),)
        self.pipelines = pipelines
        self.methods: Dict[str, Deque[Callable]] = defaultdict(deque)
        for pipeline in pipelines:
            self._add_pipeline(pipeline)

        self.crawler = crawler
        self.max_items = max_items
        self.items = deque()
        self.processing = []
        self.running = False

    @classmethod
    def from_settings(cls, settings, crawler=None):
        pipelines = []
        pipeline_setting = settings.get("ITEM_PIPELINES")
        cls_paths = [k for k, v in sorted(pipeline_setting.items(), key=itemgetter(1))]
        for cls_path in cls_paths:
            pipeline_cls = symbol_by_name(cls_path)
            pipeline = build_from_crawler(pipeline_cls, crawler)
            pipelines.append(pipeline)
        return cls(*pipelines, crawler=crawler, max_items=settings.getint('ITEM_PROCESSOR_MAX_ITEMS', 1))

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings, crawler)

    def _add_pipeline(self, pipeline):
        if hasattr(pipeline, 'close'):
            self.methods['close'].appendleft(pipeline.close)
        if hasattr(pipeline, "process_item"):
            self.methods["process_item"].append(pipeline.process_item)
        if hasattr(pipeline, "process_items"):
            self.methods["process_items"].append(pipeline.process_items)

    def active(self):
        return len(self.items) + len(self.processing) > 0

    def process_queue(self):
        wait_time = time.time()
        self.running = True
        while self.running:
            try:
                item = self.items.pop()
                self.process_item(item)
                self.processing.append(item)
                if len(self.processing) >= self.max_items:
                    self.process_items(self.processing)
                    self.processing[:] = []
                wait_time = time.time()
            except IndexError:
                if self.processing and time.time() - wait_time > 1:
                    self.process_items(self.processing)
                    self.processing[:] = []
            finally:
                self.crawler.sleep(0.001)
        if self.processing:
            self.process_items(self.processing)

    def process_item_future(self, item):
        self.items.append(item)

    def process_item(self, item):
        for method in self.methods.get("process_item", []):
            method(item)

    def process_items(self, items):
        for method in self.methods.get("process_items", []):
            method(items)

    def close(self):
        for close_func in self.methods.get("close", []):
            close_func()
        self.methods.clear()
        self.items.clear()
        self.processing.clear()
        self.running = False
