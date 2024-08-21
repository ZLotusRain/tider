import time
import logging
import weakref
from collections import deque

from tider.network import Request
from tider.utils.misc import evaluate_callable

logger = logging.getLogger(__name__)

PENDING = -1
SCHEDULED = 1
EXECUTED = 2
RESOLVED = 3
REJECTED = 4


class PromiseNode:
    """The promise node which binds to a request task.

    :param request: (optional) the instance of :class:`Request`
    :param parent: (optional).

    """

    __slots__ = (
        'request',
        'url',
        'parent',
        'promise',
        'children',
        'invalid_children',
        'rejected_reason',
        '_pending',
        'state',
        '_is_root',
        '__weakref__'
    )

    def __init__(self, request=None, parent=None, promise=None):
        # node tree and requests
        self.request = request
        if request:
            self.url = request.url
        else:
            self.url = None
        self.parent = parent
        if parent:
            self.promise = promise or parent.promise
        else:
            self.promise = promise

        self.children = []
        self.invalid_children = []
        self.rejected_reason = None
        self._pending = deque(maxlen=1)
        self._pending.append(1)
        self.state = PENDING
        self._is_root = False if parent else True

    @property
    def root(self):
        """
        find the nearest root
        """
        node = self
        while not node._is_root:
            node = node.parent
        return node

    @property
    def rejected(self):
        if self.state == REJECTED:
            return True
        elif any([child.rejected for child in self.children]):
            return True
        return False

    @property
    def size(self):
        size = len(self.children)
        for child in self.children:
            size += child.size
        return size

    @property
    def unresolved_children(self):
        return list(filter(lambda x: isinstance(x, PromiseNode) and x.state < RESOLVED,
                           [child for child in self.children]))

    @property
    def next_nodes(self):
        """Schedule every pending child."""

        while self.invalid_children:
            yield self.invalid_children.pop()
        for child in self.unresolved_children:
            yield from child.next_nodes  # update child state
        if self.state == PENDING:
            try:
                self._pending.pop()
                if self.request:
                    self.state = SCHEDULED
                    yield self
                else:
                    self.state = EXECUTED  # root
            except IndexError:
                pass
        if self.state == EXECUTED and not self.children:
            self.state = RESOLVED
            if self.parent and self in self.parent.children:
                self.parent.children.remove(self)
            self.clear()

    def add_child(self, request):
        if request.meta.get('promise_node'):
            promise_node = request.meta.get('promise_node')
            node = promise_node.root
            if not node.parent:
                node.parent = self  # add parent promise, do not use proxy here
            promise_node.request = request  # copy request state to avoid lose cb_kwargs
            promise_node.reset()  # SCHEDULED to PENDING
        else:
            node = PromiseNode(request=request, parent=weakref.proxy(self), promise=self.promise)
        if node not in self.children:
            self.children.append(node)

    def update_state(self):
        for child in self.children:
            child.update_state()
        if self.state == EXECUTED and not self.children:
            self.state = RESOLVED
            if self.parent and self in self.parent.children:
                self.parent.children.remove(self)
            self.clear()

    def then(self):
        if self.promise:
            yield from self.promise.then()

    def reset(self):
        self._pending = deque(maxlen=1)
        self._pending.append(1)
        self.state = PENDING

    def clear(self):
        # do not clear parent and promise here if is root
        # to avoid break conn with parent promise.
        request, self.request = self.request, None
        request and request.close()
        self.children.clear()
        self.invalid_children.clear()

    def __str__(self):
        if self._is_root:
            return f'<Promise root at 0x{id(self):0x}>'
        else:
            return f'<Promise node {self.request} at 0x{id(self):0x}>'

    __repr__ = __str__


class Promise:
    """Ensure completeness and sequence of the target which relies on multi async tasks.\n
    Bound Executor: \n
    tider.core.explorer | tider.core.parser \n
    Bound Scheduler: \n
    tider.core.parser | tider.core.scheduler \n

    :param reqs: an iterable object which contains the instance of :class:`Request`
    :param callback: (optional) callback after all the async tasks are done.
    :param cb_kwargs: (optional)
    :param delay: (optional) the interval between each schedule.
    :param values: (optional) key-value pairs passed between nodes.

    Usage::
    """

    def __init__(self, reqs, callback, cb_kwargs=None, values=None, delay=None):
        if isinstance(reqs, Request):
            reqs = [reqs]
        else:
            if not all([isinstance(req, Request) for req in reqs]):
                raise ValueError('param `reqs` must be the list of `Request`')
            reqs = reqs
        root = PromiseNode(request=None, promise=self)
        for idx, req in enumerate(reqs):
            req.meta['promise_order'] = idx
            root.add_child(req)  # the address of reqs can't be the same.
        self.root = root

        self.callback = evaluate_callable(callback)
        self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}
        self.values = dict(values) if values else {}

        self.delay = delay
        self.retry_times = 0

        self._unresolved = deque(maxlen=1)
        self._unresolved.append(1)
        self._state = PENDING

    @property
    def state(self):
        if self.root:
            self._state = self.root.state
        return self._state

    @property
    def size(self):
        size = 0
        if self.root is not None:
            size = self.root.size
        return size

    def reject(self):
        self.root.state = REJECTED

    def clear(self):
        self.root.clear()
        del self.callback
        self.cb_kwargs.clear()
        self.values.clear()

    def then(self):
        """
        Schedule every pending child.
        If every node is resolved, then the promise is resolved.
        """

        for node in self.root.next_nodes:
            if not isinstance(node, PromiseNode):
                yield node
            else:
                if node.state == REJECTED:
                    logger.warning(f"Promise node {node} is rejected, "
                                   f"reason: {node.rejected_reason}")
                else:
                    request = node.request.copy()  # copy when node is set
                    if not request.meta.get("promise_node"):
                        # avoid parent promise overwrites child promise.
                        request.meta["promise_node"] = node
                    request.cb_kwargs.update(self.values)
                    yield request
            if self.delay:
                time.sleep(self.delay)
        self.root.update_state()  # avoid misstate
        if self.state >= RESOLVED:
            yield from self.on_resolved()

    def on_resolved(self):
        try:
            self._unresolved.pop()  # avoid multi nodes resolving
        except IndexError:
            return
        if self.state == RESOLVED:
            kwargs = self.cb_kwargs.copy()
            # values in node should be processed
            kwargs.update(self.values)
            results = self.callback(**kwargs)
            try:
                for result in results or []:
                    yield result
            except Exception as e:
                logger.error(f'Promise bug when resolving: {e}', exc_info=True)
                self.reject()
            if hasattr(results, 'close'):
                results.close()
        if self.root.parent is not None:
            yield from self.root.parent.then()
        else:
            self.clear()


def dummy_validator(**kwargs):
    return True


class AntiCrawlPromise(Promise):

    def __init__(self, reqs, validator, max_retries=None, **kwargs):
        super().__init__(reqs=reqs, **kwargs)

        self.reqs = [req.replace() for req in reqs]
        self.validator = evaluate_callable(validator) or dummy_validator
        self.max_retries = max_retries
        self.retry_times = 0

    def recreate_root(self):
        reqs = [req.replace() for req in self.reqs]
        root = PromiseNode(request=None, promise=self)
        for idx, req in enumerate(reqs):
            req.meta['promise_order'] = idx
            root.add_child(req)  # the address of reqs can't be the same.
        self.root = root

    def on_resolved(self):
        try:
            self._unresolved.pop()  # avoid multi nodes resolving
        except IndexError:
            return
        if self.state == RESOLVED:
            if not self.validator(**self.values):
                if self.max_retries is None or self.retry_times < self.max_retries:
                    self.recreate_root()
                    self.retry_times += 1
                    yield from self.then()
            else:
                super().on_resolved()
        if self.root.parent is not None:
            yield from self.root.parent.then()
        self.clear()
