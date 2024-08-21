import sys
import time
import logging
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
    if not hasattr(sys, 'pypy_version_info'):  # pragma: no cover
        __slots__ = (
            'request', 'parent', 'children', 'invalid_children',
            'rejected_reason', '_pending', 'state',
            '_is_root', '_str', '__weakref__', '__dict__'
        )

    def __init__(self, request=None, parent=None, promise=None):
        # node tree and requests
        self.request = request
        self.parent = parent
        self.promise = promise

        self.children = []
        self.invalid_children = []

        self.rejected_reason = None
        self.state = PENDING
        self._pending = deque((1, ), maxlen=1)
        self._str = str(request) if request else None
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
        try:
            self._pending.pop()
            if self.state == PENDING:
                if self.request:
                    self.state = SCHEDULED
                    yield self
                else:
                    self.state = EXECUTED  # root
        except IndexError:
            pass
        if self.state == EXECUTED and not self.children:
            if not self._is_root and self.state not in (RESOLVED, REJECTED):
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
            # # use this request to update promise values.
            # old_request, promise_node.request = promise_node.request, request  # copy request state to avoid lose cb_kwargs
            # old_request.close()
            # promise_node.reset()  # SCHEDULED to PENDING
            promise_node.merge_values(request)
        else:
            node = PromiseNode(request=request, parent=self, promise=self.promise)
        if node not in self.children:
            self.children.append(node)

    def update_state(self):
        for child in self.children:
            child.update_state()
        if self.state == EXECUTED and not self.children:
            if not self._is_root:
                self.state = RESOLVED
                if self.parent and self in self.parent.children:
                    self.parent.children.remove(self)
                self.clear()

    def then(self):
        yield from self.root.promise.then()

    def merge_values(self, request):
        root_parent = self.root.parent
        while root_parent:
            # if schedule happens in child promise, then the node in
            # child promise can't be updated with values in parent promise.
            # so we need to update values here.
            for k, v in root_parent.promise.values.items():
                request.cb_kwargs.setdefault(k, v)
            root_parent = root_parent.root.parent
        request.cb_kwargs.update(self.root.promise.values)

    def clear(self):
        # do not clear parent and promise here if is root
        # to avoid break conn with parent promise.
        request, self.request = self.request, None
        request and request.close()

        # break references
        self.parent = None
        self.promise = None

        self.children[:] = []
        self.invalid_children[:] = []

    def __str__(self):
        if self._is_root:
            return f'<Promise root at 0x{id(self):0x}>'
        else:
            return f'<Promise node {self._str} at 0x{id(self):0x}>'

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

        root = PromiseNode(promise=self)
        # avoid consuming generator
        for idx, req in enumerate(reqs):
            if not isinstance(req, Request):
                raise ValueError('param `reqs` must be the list of `Request`')
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
        root, self.root = self.root, None
        root.clear()
        del self.callback, root
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

                    # use node instead of self to update from child promise.
                    root_parent = node.root.parent
                    while root_parent:
                        for k, v in root_parent.promise.values.items():
                            request.cb_kwargs.setdefault(k, v)
                        root_parent = root_parent.root.parent
                    request.cb_kwargs.update(node.promise.values)
                    yield request
            if self.delay:
                time.sleep(self.delay)
        # if promise has already resolved, then skip this.
        if self.root is None:
            return
        # self.root.update_state()  # avoid misstate
        if self.state == EXECUTED and not self.root.children:
            yield from self.on_resolved()

    def on_resolved(self):
        try:
            self._unresolved.pop()  # avoid multi nodes resolving
        except IndexError:
            return
        # if self.state == RESOLVED:
        kwargs = self.cb_kwargs.copy()
        # values in node should be processed
        kwargs.update(self.values)
        results = self.callback(**kwargs)
        try:
            for result in results or []:
                # schedule anyway to avoid lose connection with parent promise.
                # maybe duplicated
                # yield result

                # keep parent promise.
                if isinstance(result, Request) and self.root.parent is not None:
                    self.root.parent.add_child(result)
                    if result.meta.get("promise_node"):
                        yield result
                else:
                    yield result
        except Exception as e:
            logger.error(f'Promise bug when resolving: {e}', exc_info=True)
            self.reject()
        if hasattr(results, 'close'):
            results.close()

        self.root.state = RESOLVED
        if self.root.parent is not None:
            # new added
            if self.root in self.root.parent.children:
                self.root.parent.children.remove(self.root)
            yield from self.root.parent.then()
        # clear root node and collect garbage.
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
