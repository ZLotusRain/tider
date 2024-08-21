"""
Module containing all HTTP related classes

Use this module (instead of the more specific ones) when importing
Request and Response outside this module.
"""

from tider.network.request import Request
from tider.network.response import Response
from tider.network.proxy import ProxyPool, ProxyPoolManager


__all__ = ('Request', 'Response', 'ProxyPool', 'ProxyPoolManager')
