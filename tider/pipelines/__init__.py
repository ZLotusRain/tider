import logging
from operator import itemgetter
from typing import Callable, Deque, Dict
from collections import defaultdict, deque

from tider.utils.misc import create_instance, symbol_by_name

logger = logging.getLogger(__name__)


class ItemPipelineManager:
    def __init__(self, *pipelines):
        self.pipelines = pipelines
        self.methods: Dict[str, Deque[Callable]] = defaultdict(deque)
        for pipeline in pipelines:
            self._add_pipeline(pipeline)

    @classmethod
    def from_settings(cls, settings, crawler=None):
        pipelines = []
        pipeline_setting = settings.get("ITEM_PIPELINES")
        cls_paths = [k for k, v in sorted(pipeline_setting.items(), key=itemgetter(1))]
        for cls_path in cls_paths:
            pipeline_cls = symbol_by_name(cls_path)
            pipeline = create_instance(pipeline_cls, settings=settings, crawler=crawler)
            pipelines.append(pipeline)
        return cls(*pipelines)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings, crawler)

    def _add_pipeline(self, pipeline):
        if hasattr(pipeline, 'close'):
            self.methods['close'].appendleft(pipeline.close)
        if hasattr(pipeline, "process_item"):
            self.methods["process_item"].append(pipeline.process_item)

    def process_item(self, item):
        if not self.methods.get("process_item"):
            logger.info(item.jsonify())
        else:
            [method(item) for method in self.methods["process_item"]]
        item.discard()
        del item

    def close(self):
        for close_func in self.methods.get("close", []):
            close_func()
        self.methods.clear()
        del self.pipelines
