# import sys
# import time
# import logging
# import weakref
# from uuid import uuid4
# from collections import deque
# from threading import Event
#
# from tider.network import Request
# from tider.utils.misc import evaluate_callable
#
# logger = logging.getLogger(__name__)
#
# PENDING = -1
# SCHEDULED = 1
# EXECUTED = 2
# RESOLVED = 3
# REJECTED = 4
#
#
# def _create_state():
#     state = deque(maxlen=1)
#     state.append(1)
#     return state
#
#
# class PromiseNodeV2:
#     """The promise node which binds to a request task.
#
#     :param request: (optional) the instance of :class:`Request`
#     :param parent: (optional).
#
#     """
#     if not hasattr(sys, 'pypy_version_info'):  # pragma: no cover
#         __slots__ = (
#             'request', 'parent', 'children', 'invalid_children',
#             'rejected_reason', '_pending', 'state',
#             '_is_root', '__weakref__', '__dict__'
#         )
#
#     promise = None
#
#     def __init__(self, request=None, parent=None, promise=None):
#         # node tree and requests
#         if parent is not None and request is None:
#             raise ValueError('Can not add an empty node.')
#         self.request = request
#         self.parent = parent
#         self.promise = promise or parent.promise if parent else promise
#
#         self.rejected_reason = None
#         self.state = PENDING
#         self._pending = _create_state()
#         # set parent after init like embedded promise
#         # won't change this property.
#         self._is_root = False if parent else True
#
#         self.children = []  # FIFO
#         self.invalid_children = []
#
#     @property
#     def root(self):
#         """
#         find the nearest or the child promise root.
#         """
#         node = self
#         while not node._is_root:
#             node = node.parent
#         return node
#
#     @property
#     def rejected(self):
#         if self.state == REJECTED:
#             return True
#         elif any([child.rejected for child in self.children]):
#             return True
#         return False
#
#     @property
#     def size(self):
#         """the number or children of this node."""
#         size = len(self.children)
#         for child in self.children:
#             size += child.size
#         return size
#
#     @property
#     def unresolved_children(self):
#         return list(filter(lambda x: isinstance(x, PromiseNode) and x.state < RESOLVED,
#                            [child for child in self.children]))
#
#     @property
#     def next_nodes(self):
#         """Schedule every pending child."""
#
#         while self.invalid_children:
#             yield self.invalid_children.pop()
#         for child in self.unresolved_children:
#             yield from child.next_nodes  # update child state
#         if self.state == PENDING:
#             try:
#                 self._pending.pop()
#                 if self.request:
#                     self.state = SCHEDULED
#                     yield self
#                 else:
#                     self.state = EXECUTED  # root
#             except IndexError:
#                 pass
#         if self.state == EXECUTED and not self.unresolved_children:
#             self.state = RESOLVED
#             if self.parent and self in self.parent.children:
#                 self.parent.children.remove(self)
#             # keep root to avoid losing connection with parent promise
#             if not self._is_root:
#                 self.clear()
#
#     def add_child(self, request):
#         if request.meta.get('promise_node'):
#             promise_node = request.meta.get('promise_node')
#             node = promise_node.root
#             if not node.parent:
#                 node.parent = self  # add parent promise, do not use proxy here
#             promise_node.request = request  # copy request state to avoid lose cb_kwargs
#             promise_node.reset()  # SCHEDULED to PENDING
#         else:
#             node = PromiseNode(request=request, parent=weakref.proxy(self), promise=self.promise)
#         if node not in self.children:
#             self.children.append(node)
#
#     def update_state(self):
#         for child in self.children:
#             child.update_state()
#         if self.state == EXECUTED and not self.children:
#             self.state = RESOLVED
#             if self.parent and self in self.parent.children:
#                 self.parent.children.remove(self)
#             self.clear()
#
#     def then(self):
#         if self.promise:
#             yield from self.promise.then()
#
#     def reset(self):
#         self._pending = _create_state()
#         self.state = PENDING
#
#     def clear(self):
#         # do not clear parent and promise here if is root
#         # to avoid break conn with parent promise.
#         request, self.request = self.request, None
#         request and request.close()
#         self.parent = None
#         self.promise = None
#         self.children.clear()
#         self.invalid_children.clear()
#
#     def __str__(self):
#         if self._is_root:
#             return f'<Promise root at 0x{id(self):0x}>'
#         else:
#             return f'<Promise node {self.request} at 0x{id(self):0x}>'
#
#     __repr__ = __str__
#
#
# class PromiseV2:
#     """Ensure completeness and sequence of the target which relies on multi async tasks.\n
#     Bound Executor: \n
#     tider.core.explorer | tider.core.parser \n
#     Bound Scheduler: \n
#     tider.core.parser | tider.core.scheduler \n
#
#     :param reqs: an iterable object which contains the instance of :class:`Request`
#     :param callback: (optional) callback after all the async tasks are done.
#     :param cb_kwargs: (optional)
#     :param delay: (optional) the interval between each schedule.
#     :param values: (optional) key-value pairs passed between nodes.
#
#     Usage::
#     """
#
#     def __init__(self, reqs, callback, cb_kwargs=None, errback=None, values=None, delay=None):
#         """Initialize the Promise into a pending state."""
#         if isinstance(reqs, Request):
#             reqs = [reqs]
#         else:
#             if not all([isinstance(req, Request) for req in reqs]):
#                 raise ValueError('param `reqs` must be the iterator of `Request`')
#             reqs = reqs
#         root = PromiseNode(request=None, promise=self)
#         for idx, req in enumerate(reqs):
#             req.meta['promise_order'] = idx
#             root.add_child(req)  # the address of reqs can't be the same.
#         self.root = root
#
#         self.callback = evaluate_callable(callback)
#         self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}
#         self.errback = evaluate_callable(errback)
#         self.values = dict(values) if values else {}
#
#         self.delay = delay
#
#         self._unresolved = _create_state()
#         self._state = PENDING
#
#     @property
#     def state(self):
#         if self.root:
#             self._state = self.root.state
#         return self._state
#
#     @property
#     def size(self):
#         size = 0
#         if self.root is not None:
#             size = self.root.size
#         return size
#
#     def reject(self):
#         self.root.state = REJECTED
#
#     def clear(self):
#         self.root.clear()
#         self.cb_kwargs.clear()
#         self.values.clear()
#
#     def then(self):
#         """
#         Schedule every pending child.
#         If every node is resolved, then the promise is resolved.
#         """
#
#         for node in self.root.next_nodes:
#             if not isinstance(node, PromiseNode):
#                 yield node
#             else:
#                 if node.state == REJECTED:
#                     logger.warning(f"Promise node {node} is rejected, "
#                                    f"reason: {node.rejected_reason}")
#                 else:
#                     request = node.request.copy()  # copy when node is set
#                     if not request.meta.get("promise_node"):
#                         # avoid parent promise overwrites child promise.
#                         request.meta["promise_node"] = node
#                     request.cb_kwargs.update(self.values)
#                     yield request
#             if self.delay:
#                 time.sleep(self.delay)
#         # self.root.update_state()  # avoid misstate
#         if self.state >= RESOLVED:
#             yield from self.on_resolved()
#
#     def on_resolved(self):
#         try:
#             self._unresolved.pop()  # avoid multi nodes resolving
#         except IndexError:
#             return
#         if self.state == RESOLVED:
#             kwargs = self.cb_kwargs.copy()
#             # values in node should be processed
#             kwargs.update(self.values)
#             callback, self.callback = self.callback, None
#             results = callback(**kwargs)
#             try:
#                 for result in results or []:
#                     yield result
#             except Exception as e:
#                 logger.error(f'Promise bug when resolving: {e}', exc_info=True)
#                 self.reject()
#             if hasattr(results, 'close'):
#                 results.close()
#         if self.root.parent is not None:
#             yield from self.root.parent.then()
#         else:
#             self.clear()
#
#
# class Pending:
#     """Object to store pending tasks in promise."""
#     def __init__(self, keep_relation=True):
#         self.keep_relation = keep_relation
#         self.children = []
#
#     def add_child(self, request):
#         if request.meta.get('promise_node'):
#             promise_node = request.meta.get('promise_node')
#             node = promise_node.root
#             if not node.parent:
#                 node.parent = self  # add parent promise, do not use proxy here
#             promise_node.request = request  # copy request state to avoid lose cb_kwargs
#             promise_node.reset()  # SCHEDULED to PENDING
#         else:
#             node = PromiseNode(request=request, parent=weakref.proxy(self), promise=self.promise)
#         if node not in self.children:
#             self.children.append(node)
#
#     @property
#     def unresolved_children(self):
#         return list(filter(lambda x: isinstance(x, PromiseNode) and x.state < RESOLVED,
#                            [child for child in self.children]))
#
#
# class PromiseNode:
#
#     promise = None
#
#     def __init__(self, request=None, promise=None, depth=0, parent=None):
#         if request:
#             request.callback = self._promise_back(request.callback)
#             request.errback = self._promise_back(request.errback) if request.errback else None
#         self.request = request
#         self.parent = parent  # 是否需要
#         self.promise = promise or self.promise
#
#         self.depth = depth
#         self.children = []
#         self._pending = deque((self.request,))
#         self._executed = deque()
#         self.done = False
#
#     def to_pending(self):
#         if self._pending:
#             raise RuntimeError('The promise node already at pending state.')
#         self._pending.append(self.request)
#
#     def on_executed(self):
#         if self._pending:
#             raise RuntimeError('Can not mark unscheduled node as executed.')
#         if self._executed:
#             raise RuntimeError('The promise node has already been executed.')
#         self.request = None
#         self._executed.append(1)
#
#     @property
#     def unresolved_children(self):
#         return list(
#             filter(lambda x: x.state < RESOLVED, [child for child in self.children]))
#
#     def then(self, reqs=None):
#         if self.promise.cancelled:
#             return
#
#         reqs = reqs or []
#         if isinstance(reqs, Request):
#             reqs = [reqs]
#         for req in reqs:
#             if not isinstance(req, Request):
#                 raise ValueError('param `reqs` must be the iterator of `Request`')
#             promise_node = req.meta.get('promise_node')
#             if not promise_node:
#                 promise_node = PromiseNode(promise=self.promise, depth=self.depth + 1, parent=self)
#                 # promise.values = self.values  # avoid copy in promise progress
#             else:
#                 # only the first layer of the embedded promise
#                 # has promise node.
#                 root = promise_node.promise.root
#                 if not root.parent:
#                     root.parent = self
#                 promise_node.to_pending()
#             # self.children.append(promise_node)
#             req.promise_node = promise_node
#             self.children.append(req)
#
#     def _promise_back(self, func):
#         def wrapped(response):
#             try:
#                 for output in func(response):
#                     if isinstance(output, Request):
#                         self.then(output)
#                     else:
#                         yield output
#                 response.request.promise.executed()
#             except Exception as e:
#                 response.request.promise.throw(e)
#                 # raise e
#             if isinstance(response, Failure):
#                 response.request.promise.throw(e)
#             yield from self
#         return wrapped
#
#     def __call__(self):
#         """Schedule every pending child."""
#         for child in self.unresolved_children:
#             yield from child  # update child state
#         try:
#             yield self._pending.pop()
#         except IndexError:
#             if not self.request:
#                 self.on_executed()  # root
#
#         if self._executed and not self.unresolved_children:
#             self.state = RESOLVED
#             self.done = True
#             # keep root to avoid losing connection with parent promise
#             if self.parent and self in self.parent.children:
#                 self.parent.children.remove(self)
#             yield from self.parent  # mind looping.
#
#     __iter__ = __call__
#
#
# class PromiseState:
#     pass
#
#
# class PromiseBack:
#
#     promise = None
#
#     def __init__(self, func):
#         self.func = weakref.WeakMethod(func)
#
#     def __call__(self, response):
#         try:
#             for output in self.func(response):
#                 if isinstance(output, Request):
#                     self.then(output)
#                 else:
#                     yield output
#             response.request.promise.executed()
#         except Exception as e:
#             response.request.promise.throw(e)
#             # raise e
#         if isinstance(response, Failure):
#             response.request.promise.throw(e)
#         yield from self
#
#
# class PromiseV3:
#
#     """
#     需要有一个存储全局配置和全局搜索pending tasks的对象
#     max_depth
#     child.depth = self.depth + 1
#     不需要每次从root开始寻找pending节点
#     """
#
#     def __init__(self, callback, cb_kwargs=None, errback=None, reqs=None, values=None,
#                  identify=uuid4, keep_relation=True, parent=None):
#         """Initialize the Promise into a pending state."""
#
#         self.callback = evaluate_callable(callback)
#         self.cb_kwargs = dict(cb_kwargs) if cb_kwargs else {}
#         self.errback = evaluate_callable(errback)
#
#         self.root = PromiseNode()
#
#         self.values = values
#         self.keep_relation = keep_relation
#
#         self.uuid = identify()
#         self.reqs = reqs
#         self.parent = parent
#         self.reason = None
#
#         # global
#         self.tokenize = None
#         self.tokenized = Event()
#         self.token = None
#         self._untokenized_requests = deque()
#
#         self._pending = deque()
#         self._unresolved = _create_state()
#         self.state = PENDING
#         self.cancelled = False
#
#     def then(self, reqs=None):
#         if self.cancelled:
#             return
#
#         reqs = reqs or []
#         if isinstance(reqs, Request):
#             reqs = [reqs]
#         else:
#             if not all([isinstance(req, Request) for req in reqs]):
#                 raise ValueError('param `reqs` must be the iterator of `Request`')
#             reqs = reqs
#         for req in reqs:
#             promise = req.promise
#             if not promise:
#                 promise = PromiseT(callback=self, request=req, parent=self)
#                 promise.values = self.values  # avoid copy in promise progress
#             self._pending.append(promise)
#
#     def child(self):
#         return PromiseT(callback=self.callback, parent=self)
#
#     def throw(self):
#         pass
#
#     def promised_request(self):
#         request = self.request
#         request.promise = weakref.proxy(self)
#         request.callback = self._promise_back(request.callback)
#         request.errback = self._promise_back(request.errback)
#         request.cb_kwargs.update(self.values)
#         return request
#
#     def _promise_back(self, func):
#         def wrapped(response):
#             try:
#                 for output in func(response):
#                     if isinstance(output, Request):
#                         self.then(output)
#                     else:
#                         yield output
#                 response.request.promise.executed()
#             except Exception as e:
#                 response.request.promise.throw(e)
#                 # raise e
#             if isinstance(response, Failure):
#                 response.request.promise.throw(e)
#             yield from self
#         return wrapped
#
#     def __call__(self):
#         if self.request:
#             yield self.promised_request()
#         while self._pending:
#             try:
#                 promise = self._pending.popleft()
#                 if not promise.request:
#                     yield from promise()
#                 else:
#                     request = promise.request
#                     request.promise = weakref.proxy(promise)
#                     request.cb_kwargs.update(self.values)
#                     yield request
#             except IndexError:
#                 pass
#         yield from self.maybe_evaluate()
#
#     __iter__ = __call__
#
#     def maybe_evaluate(self):
#         if self.state >= RESOLVED:
#             yield from self.on_resolved()
#
#     def on_resolved(self):
#         try:
#             self._unresolved.pop()  # avoid multi nodes resolving
#         except IndexError:
#             return
#         if self.state == RESOLVED:
#             kwargs = self.cb_kwargs.copy()
#             # values in node should be processed
#             kwargs.update(self.values)
#             callback, self.callback = self.callback, None
#             results = callback(**kwargs)
#             try:
#                 for result in results or []:
#                     yield result
#             except Exception as e:
#                 logger.error(f'Promise bug when resolving: {e}', exc_info=True)
#                 self.reject()
#             if hasattr(results, 'close'):
#                 results.close()
#         if self.root.parent is not None:
#             yield from self.root.parent.then()
#         else:
#             self.clear()
#
#
# Promise = PromiseV3
#
#
# def gen_promise():
#     promise = Promise()
#     yield from promise.then(reqs=reqs)
#
#
# def parse(response):
#     promise = response.promise
#     yield from promise.then(reqs=Promise().then(reqs))
#
#
#
# class Promise:
#
#     def __init__(self, callback, args=None, cb_kwargs=None, values=None, delay=None, depth_limit=None):
#         self.parent = None
#         self.depth = 0
#         self._pending = deque()
#
#     def then(self, requests):
#         for request in requests:
#             request.promise = Promise(callback=self, args=)
#             request.promise.parent = self
#
#     def __call__(self, *args, **kwargs):
#         while self._pending:
#             pending = self._pending.pop()
