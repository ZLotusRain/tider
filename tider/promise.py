import sys
import time
import logging
import weakref
import inspect
from collections import deque
from enum import Enum

from tider.network import Request
from tider.utils.misc import evaluate_callable
from tider.utils.functional import noop, iter_generator

__all__ = ('is_unresolved', 'is_pending', 'is_scheduled',
           'is_executed', 'is_rejected', 'is_resolved', 'Promise', 'NodeState')

logger = logging.getLogger(__name__)


class NodeState(Enum):
    PENDING = -1
    SCHEDULED = 1
    EXECUTED = 2
    RESOLVED = 3
    REJECTED = 4


def is_unresolved(state: NodeState):
    return state.value < NodeState.RESOLVED.value


def is_pending(state: NodeState):
    return state == NodeState.PENDING


def is_scheduled(state: NodeState):
    return state == NodeState.SCHEDULED


def is_executed(state: NodeState):
    return state == NodeState.EXECUTED


def is_rejected(state: NodeState):
    return state == NodeState.REJECTED


def is_resolved(state: NodeState):
    return state == NodeState.RESOLVED


class PromiseNode:
    """The promise node which binds to a request task."""
    if not hasattr(sys, 'pypy_version_info'):  # pragma: no cover
        __slots__ = (
            'request', 'parent', 'children', 'invalid_children',
            'rejected_reason', '_pending', 'state', 'source',
            '_is_root', '_str', '__weakref__', '__dict__'
        )

    def __init__(self, request=None, parent=None, promise=None, source=None):
        # node tree and requests
        self.request = request
        if source and not hasattr(source, '__iter__'):
            raise ValueError('Requests source must be iterable.')
        self.source = source

        self.promise = promise

        self.parent = parent
        self.children = []
        self.invalid_children = []

        self.rejected_reason = None
        self.state = NodeState.PENDING
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
        return any([is_rejected(self.state)] + [is_rejected(child.state) for child in self.children])

    @property
    def size(self):
        size = len(self.children)
        for child in self.children:
            size += child.size
        return size

    @property
    def unresolved_children(self):
        return list(filter(
            lambda x: isinstance(x, PromiseNode) and is_unresolved(x.state), [child for child in self.children]
        ))

    def add_child(self, request):
        if isinstance(request, Request):
            promise_node = request.meta.get('promise_node')
            if promise_node:
                # # use this request to update promise values.
                # # copy request state to avoid lose cb_kwargs
                node = promise_node.root
                if node.parent and node.parent is not self:
                    raise ValueError("Can't add root with parent.")
                node.parent = self  # add parent promise, do not use proxy here
                # old_request, promise_node.request = promise_node.request, request
                # old_request.close()
                # promise_node.reset()  # SCHEDULED to PENDING
                promise_node.merge_values(request)
            else:
                node = PromiseNode(request=request, parent=self)
            children = self.children
        else:
            node = request
            children = self.invalid_children
        if node not in children:
            children.append(node)
        return node

    @property
    def next_nodes(self):
        """Schedule every pending child."""
        while self.invalid_children:
            yield self.invalid_children.pop()

        if is_resolved(self.state):
            # avoid processing resolved node due to state changed
            # after evaluating unresolved children.
            return

        try:
            self._pending.pop()
            self.state = NodeState.SCHEDULED
        except IndexError:
            if is_executed(self.state):
                self.on_executed()
        else:
            if not self._is_root:
                yield self
            else:
                self.state = NodeState.EXECUTED  # root
                order = 0
                for output in iter_generator(self.source):
                    if not isinstance(output, Request):
                        node = output
                    else:
                        if 'promise_order' not in output.meta:
                            output.meta['promise_order'] = order
                        order += 1
                        # the address of reqs can't be the same,
                        # otherwise the node will be ignored.
                        self.add_child(output)
                        node = output.meta.get('promise_node')  # already SCHEDULED
                    if not node:
                        continue
                    # unresolved children only be evaluated once
                    # so state change won't affect the source promise.
                    yield node
                self.source = None  # don't delete source here to make sure the memory will be released.
        for child in self.unresolved_children:
            yield from child.next_nodes

    def update_state(self):
        for child in self.children:
            child.update_state()
        if is_executed(self.state):
            self.on_executed()

    def then(self):
        return self.root.promise.then()

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

    def on_executed(self):
        if not self._is_root:
            if self.root.promise == self.promise:
                self.promise = None
            if not self.children and self.state not in (NodeState.RESOLVED, NodeState.REJECTED):
                self.state = NodeState.RESOLVED
                self.clear()
        request, self.request = self.request, None
        request and request.close()

    def clear(self):
        # do not clear parent and promise here if is root
        # to avoid break conn with parent promise.
        request, self.request = self.request, None
        request and request.close()

        # break references
        try:
            if self.parent and self in self.parent.children:
                self.parent.children.remove(self)
            self.parent = None
            self.promise = None
        except AttributeError:
            pass

        del self.parent, self.promise  # no need to delete request here.
        del self.children[:], self.invalid_children[:]

    def __str__(self):
        if self._is_root:
            return f'<Promise root at 0x{id(self):0x}: {self.state.name}>'
        else:
            return f'<Promise node {self._str} at 0x{id(self):0x}: {self.state.name}>'

    __repr__ = __str__


class Promise:
    """Make sure the completeness and sequence of the information integration in multi layer async tasks.

    :param reqs: an iterable object which contains the instance of :class:`Request`
    :param callback: (optional) callback after all the async tasks are done.
    :param cb_kwargs: (optional)
    :param values: (optional) key-value pairs passed between nodes.
    :param delay: (optional) the interval between each schedule.
    """

    def __init__(self, reqs, callback=None, cb_kwargs=None, values=None, delay=None, spider=None):
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
            self.callback = callback or noop
        self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}

        self.delay = delay
        self.retry_times = 0
        self.spider = spider

        self._unresolved = deque((1, ), maxlen=1)
        self._state = NodeState.PENDING

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
        self.root.state = NodeState.REJECTED

    def clear(self):
        self.values.clear()
        self.cb_kwargs.clear()
        self.spider = None
        self.root.clear()
        # Avoid a refcycle if the promise is running a function with
        # an argument that has a member that points to the promise.
        del self.callback, self.values, self.cb_kwargs

    def then(self):
        """
        Schedule every pending child.
        If every node is resolved, then the promise is resolved.
        """
        for node in self.root.next_nodes:
            if not isinstance(node, PromiseNode):
                yield node  # invalid child
            elif is_rejected(node.state):
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
        if self.root is None:
            # if promise has already resolved, then skip this.
            return
        self.root.update_state()  # avoid misstate
        if is_executed(self.state) and not self.root.children:
            for result in iter_generator(self.on_resolved()):
                yield result  # iter self.on_resolved() directly.

    def on_resolved(self):
        try:
            self._unresolved.pop()  # avoid multi nodes resolving
        except IndexError:
            return
        kwargs = self.cb_kwargs.copy()
        kwargs.update(self.values)  # values in node should be processed
        callback = self.callback() if isinstance(self.callback, weakref.ReferenceType) else self.callback
        # # clen parent promise before spawn child nodes.
        try:
            for result in iter_generator(callback(**kwargs)):
                if result is None:
                    continue
                # keep parent promise.
                if isinstance(result, Request) and self.root.parent is not None:
                    self.root.parent.add_child(result)
                    if result.meta.get("promise_node"):
                        yield result  # already SCHEDULED in result's parent promise(not this parent).
                else:
                    # schedule anyway to avoid lose connection with parent promise
                    # maybe duplicated because some requests maybe already scheduled.
                    yield result
        except Exception:
            self.reject()  # reserved
            raise
        finally:
            del callback, kwargs

            self.root.state = NodeState.RESOLVED
            if self.root.parent is not None:
                # turn to parent promise
                if self.root in self.root.parent.children:
                    self.root.parent.children.remove(self.root)
                for request in iter_generator(self.root.parent.then()):
                    yield request
            # clear root node and collect garbage.
            self.clear()
