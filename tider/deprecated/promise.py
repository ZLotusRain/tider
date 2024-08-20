import logging
from inspect import isgenerator

from tider.network import Request
from tider.network import Response, Failure
from tider.core.parser import evaluate_iterable
from tider.utils.misc import symbol_by_name


logger = logging.getLogger(__name__)

PENDING = -1
SCHEDULED = 1
EXECUTED = 2
RESOLVED = 3
REJECTED = 4


class DeprecatedRequestNode:
    def __init__(self, request=None, parent=None, value=None, value_key="item"):
        if request:
            cb_kwargs = request.cb_kwargs
            if value is not None:
                cb_kwargs[value_key] = value
            self.request = request.replace(cb_kwargs=cb_kwargs)
            self.executor = request.callback
            self.errback = request.errback
        else:
            self.executor = None
            self.errback = None
            self.request = None

        self.parent = parent
        self.children = []
        self.invalid_children = []
        self.state = PENDING

    def execute(self, response):
        """
        If this action generates no children, then the current node becomes the leaf.
        A leaf can be removed after traversed.
        """
        _success = True
        iter_results = []
        if isinstance(response, Response):
            if self.executor:
                iter_results = self.executor(response=response)
        elif self.errback:
            iter_results = self.errback(failure=response)
        else:
            _success = False
            logger.error(f"Received {response}, skip: {response.reason}")
        iter_results = evaluate_iterable(iter_results)
        for result in iter_results:
            if isinstance(result, Request):
                self.children.append(DeprecatedRequestNode(request=result, parent=self))
            elif result is None:
                pass
            else:
                self.invalid_children.append(result)
        self.state = EXECUTED if _success else REJECTED

    @property
    def unscheduled_children(self):
        return list(filter(lambda x: isinstance(x, DeprecatedRequestNode) and x.state == PENDING,
                           [child for child in self.children]))

    def schedule(self):
        if self.invalid_children:
            return self.invalid_children.pop()
        elif self.unscheduled_children:
            return self.unscheduled_children[0].schedule()
        elif self.state == PENDING:
            if self.request:
                self.state = SCHEDULED
                return self
            else:
                self.state = EXECUTED
        if self.state == EXECUTED and all([child.state >= EXECUTED for child in self.children]):
            self.state = RESOLVED

    def __str__(self):
        if self.request:
            return f"<Node {str(self.request).strip('<>')}>"
        else:
            return f"<Node root>"

    __repr__ = __str__


class DeprecatedPromise:
    def __init__(self, reqs, callback=None, cb_kwargs=None, value=None, value_key="item"):
        logger.warning("tider.promise.DeprecatedPromise is deprecated, "
                       "please use tider.promise.Promise instead")

        if isinstance(reqs, Request):
            self.reqs = [reqs]
        else:
            if not all([isinstance(req, Request) for req in reqs]):
                raise ValueError(f"All elements of the `reqs` must be the instance of `Request`")
            self.reqs = reqs

        self.root = DeprecatedRequestNode(request=None, value=value, value_key=value_key)
        self.leaf = self.root
        for idx, req in enumerate(self.reqs):
            self.root.children.append(
                DeprecatedRequestNode(request=req, parent=self.root, value=value, value_key=value_key)
            )

        if callable(callback):
            self.callback = callback
        elif isinstance(callback, str):
            self.callback = symbol_by_name(callback)
        else:
            self.callback = None
        self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}

        self.value = value
        self._load = True

    def add_child(self, request):
        self.leaf.children.append(DeprecatedRequestNode(request=request, parent=self.leaf))

    def clear(self):
        self.root = None
        self.leaf = None

    def then(self):
        """Completed when all nodes have been resolved"""
        while self.leaf:
            result = self.schedule()
            if not result:
                self.leaf = self.leaf.parent
            else:
                return result
        self.clear()
        if self.callback:
            if self.value:
                return self.callback(self.value, **self.cb_kwargs)
            else:
                return self.callback(**self.cb_kwargs)
        else:
            return self.value

    def _then(self, response):
        self.execute(response)
        obj = self.then()
        if isgenerator(obj):
            yield from obj
        else:
            yield obj

    def execute(self, response):
        self.leaf.execute(response)
        if self.leaf.unscheduled_children:
            self.leaf = self.leaf.unscheduled_children[0]

    def schedule(self):
        result = self.leaf.schedule()
        if isinstance(result, DeprecatedRequestNode):
            self.leaf = result
            request = self.leaf.request.replace(callback=self._then, errback=self._then)
            return request
        return result


class RequestNode:
    """The event/task node.

    :param request: (optional) the instance of :class:`Request`
    :param parent: (optional).

    Usage::
    """

    __slots__ = (
        'request',
        'parent',
        'children',
        'invalid_children',
        'rejected_reason',
        'retried',
        'state'
    )

    def __init__(self, request=None, parent=None):
        self.request = request

        self.parent = parent
        self.children = []
        self.invalid_children = []
        self.rejected_reason = None
        self.retried = False  # flag to avoid brothers retry again.
        self.state = PENDING

    @property
    def unresolved_children(self):
        return list(filter(lambda x: isinstance(x, RequestNode) and x.state < RESOLVED,
                           [child for child in self.children]))

    @property
    def rejected(self):
        if self.state == REJECTED:
            return True
        elif any([child.rejected for child in self.children]):
            return True
        return False

    def execute(self, response):
        """
        State transition triggered after the request has been explored.
        """
        _success = True
        callback = self.request.callback
        errback = self.request.errback
        if isinstance(response, Failure):
            if errback:
                iter_results = errback(response)
            else:
                iter_results = []
        else:
            iter_results = callback(response)
        iter_results = iter_results or []

        order = 0
        for result in iter_results:
            if isinstance(result, Request):
                result.promise_order = order
                order += 1
                self.children.append(RequestNode(request=result, parent=self))
            elif isinstance(result, Failure):
                # failures from parse node point to the same request
                self.rejected_reason = result.reason
                # parent = self.parent
                # while parent.parent is not None:
                #     parent = parent.parent
                # cb_kwargs = copy.deepcopy(parent.request.cb_kwargs)
                # request = parent.request.replace(cb_kwargs=cb_kwargs.update(self.retry_kwargs))
                # self.children.append(RequestNode(request=request, parent=weakref.proxy(self),
                #                                  order_key=self.order_key, order=order,
                #                                  values=parent.values))
                _success = False
                # break
            elif result is None:
                pass
            else:
                self.invalid_children.append(result)
        self.state = EXECUTED if _success else REJECTED

    def schedule(self):
        """Schedule every pending child."""

        while self.invalid_children:
            yield self.invalid_children.pop()
        for child in self.unresolved_children:
            yield from child.schedule()
        if self.state == PENDING:
            if self.request:
                self.state = SCHEDULED
                yield self
            else:
                # root
                self.state = EXECUTED
                self.on_executed()
        if self.state == EXECUTED and not self.unresolved_children:
            self.state = RESOLVED
            self.on_resolved()

    def on_executed(self):
        self.request = None

    def on_resolved(self):
        if self.parent:
            self.parent.children.remove(self)
        self.children.clear()

    def __str__(self):
        if self.request:
            return f"<Node {str(self.request).strip('<>')}>"
        else:
            return f"<Node root>"

    __repr__ = __str__
