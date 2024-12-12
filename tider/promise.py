import sys
import time
import weakref
import inspect
import logging
from collections import deque

from tider.network import Request
from tider.utils.misc import evaluate_callable
from tider.utils.functional import iter_generator

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
            'rejected_reason', '_pending', 'state', 'source',
            '_is_root', '_str', '__weakref__', '__dict__'
        )

    def __init__(self, request=None, parent=None, promise=None, source=None):
        # node tree and requests
        self.request = request
        self.parent = parent
        self.promise = promise

        if source and not hasattr(source, '__iter__'):
            raise ValueError('Requests source must be iterable.')
        self.source = source
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

    def add_child(self, request):
        if request.meta.get('promise_node'):
            promise_node = request.meta.get('promise_node')
            node = promise_node.root
            if not node.parent:
                node.parent = self  # add parent promise, do not use proxy here
            # # use this request to update promise values.
            # # copy request state to avoid lose cb_kwargs
            # old_request, promise_node.request = promise_node.request, request
            # old_request.close()
            # promise_node.reset()  # SCHEDULED to PENDING
            promise_node.merge_values(request)
        else:
            node = PromiseNode(request=request, parent=self)
        if node not in self.children:
            self.children.append(node)

    @property
    def next_nodes(self):
        """Schedule every pending child."""
        if self.state == RESOLVED:
            # avoid processing resolved node due to state changed after
            # evaluating unresolved children.
            if self.parent and self in self.parent.children:
                self.parent.children.remove(self)
            return

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
                    for idx, req in enumerate(iter_generator(self.source)):
                        if not isinstance(req, Request):
                            raise ValueError('Requests source contains non-Request object.')
                        if 'promise_order' not in req.meta:
                            req.meta['promise_order'] = idx
                        # the address of reqs can't be the same,
                        # otherwise the node will be ignored.
                        self.add_child(req)
                        if req.meta.get('promise_node'):
                            # unresolved children only be evaluated once
                            # so state change won't affect the source promise.
                            node = req.meta['promise_node']   # already SCHEDULED
                            yield node
                    for child in self.unresolved_children:
                        yield from child.next_nodes
                    if hasattr(self.source, 'close'):
                        self.source.close()
                    self.source = None  # don't delete source here to make sure the memory will be released.
        except IndexError:
            pass
        if self.state == EXECUTED:
            request, self.request = self.request, None
            request and request.close()
            if not self._is_root and self.root.promise == self.promise:
                self.promise = None
            if not self.children:
                if not self._is_root and self.state not in (RESOLVED, REJECTED):
                    self.state = RESOLVED
                    if self.parent and self in self.parent.children:
                        self.parent.children.remove(self)
                    self.clear()

    def update_state(self):
        for child in self.children:
            child.update_state()
        if self.state == EXECUTED:
            request, self.request = self.request, None
            request and request.close()
            if not self._is_root and self.root.promise == self.promise:
                self.promise = None
            if not self.children:
                if not self._is_root and self.state not in (RESOLVED, REJECTED):
                    self.state = RESOLVED
                    if self.parent and self in self.parent.children:
                        self.parent.children.remove(self)
                    self.clear()

    def then(self):
        yield from self.root.promise.then()  # maybe use lower memory if using `yield from` directly.

    def merge_values(self, request):
        parent = self.root.parent
        while parent:
            # if schedule happens in child promise, then the node in
            # child promise can't be updated with values in parent promise.
            # so we need to update values here.
            for k, v in parent.root.promise.values.items():
                request.cb_kwargs.setdefault(k, v)
            parent = parent.root.parent
        request.cb_kwargs.update(self.root.promise.values)

    def clear(self):
        if self.state == RESOLVED:
            return
        # do not clear parent and promise here if is root
        # to avoid break conn with parent promise.
        request, self.request = self.request, None
        request and request.close()

        # break references
        self.parent = None
        self.promise = None

        del self.parent, self.promise
        del self.children[:], self.invalid_children[:]  # no need to delete request here.

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

        # init first to avoid can't access values when merging values
        # if reqs come from promise.
        self.values = dict(values) if values else {}

        # avoid consuming generator
        self.root = PromiseNode(promise=self, source=reqs)

        callback = evaluate_callable(callback)
        if inspect.ismethod(callback):
            self.callback = weakref.WeakMethod(callback)
        else:
            self.callback = callback
        self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}

        self.delay = delay
        self.retry_times = 0

        self._unresolved = deque((1, ), maxlen=1)
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

        self.values.clear()
        self.cb_kwargs.clear()
        # Avoid a refcycle if the promise is running a function with
        # an argument that has a member that points to the promise.
        del self.callback, self.values, self.cb_kwargs

    def then(self):
        """
        Schedule every pending child.
        If every node is resolved, then the promise is resolved.
        """
        for node in iter_generator(self.root.next_nodes):
            if not isinstance(node, PromiseNode):
                yield node  # invalid child
                continue

            if node.state == REJECTED:
                logger.warning(f"{node} is rejected, reason: {node.rejected_reason}")
            else:
                request = node.request.copy()  # copy when node is set
                if not request.meta.get("promise_node"):
                    # avoid parent promise overwrites child promise.
                    request.meta["promise_node"] = node
                # use node instead of self to update from child promise.
                node.merge_values(request)
                yield request

            if self.delay:
                time.sleep(self.delay)
        # if promise has already resolved, then skip this.
        if self.root is None:
            return
        self.root.update_state()  # avoid misstate
        if self.state == EXECUTED and not self.root.children:
            for result in iter_generator(self.on_resolved()):
                yield result  # iter self.on_resolved() directly.

    def on_resolved(self):
        try:
            self._unresolved.pop()  # avoid multi nodes resolving
        except IndexError:
            return
        kwargs = self.cb_kwargs.copy()
        # values in node should be processed
        kwargs.update(self.values)
        callback = self.callback() if isinstance(self.callback, weakref.ReferenceType) else self.callback
        try:
            for result in iter_generator(callback(**kwargs)):
                # # schedule anyway to avoid lose connection with parent promise.
                # # maybe duplicated
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

        del callback

        self.root.state = RESOLVED
        if self.root.parent is not None:
            # turn to parent promise
            if self.root in self.root.parent.children:
                self.root.parent.children.remove(self.root)
            for request in iter_generator(self.root.parent.then()):
                yield request
        # clear root node and collect garbage.
        self.clear()
