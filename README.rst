=====
Tider
=====

.. image:: https://img.shields.io/badge/Linux-passing-brightgreen
   :target: https://img.shields.io/badge/Linux-passing-brightgreen
   :alt: Linux

.. image:: https://img.shields.io/badge/macOS-passing-brightgreen
   :target: https://img.shields.io/badge/macOS-passing-brightgreen
   :alt: macOS

.. image:: https://img.shields.io/badge/Windows-passing-brightgreen
   :target: https://img.shields.io/badge/Windows-passing-brightgreen
   :alt: Windows


:Author: LotusRain
:Version: 3.4.0
:Source: https://github.com/ZLotusRain/tider


Overview
========

Tider 是新一代的网络爬虫框架（web crawling and scraping framework），集高性能与高扩展性于一身，
支持分布式任务处理、远程控制、HTTP/2、指纹模拟、浏览器模拟、代理管理、报警监控和文件持久化等能力。

Tider 同时支持类 Scrapy 的异步编程方式和更直观的同步式请求流程，用户可以根据任务规模、I/O 特征和部署方式
选择 gevent、thread 等不同并发池，并按需替换下载器、调度器、Broker、Pipeline 等核心组件。


Requirements
============

- Python 3.10+
- Works on Linux, Windows and macOS
- Recommended concurrency pool: ``gevent``


Installation
============

Tider 目前仍以 Git 仓库分发为主。新增 ``pyproject.toml`` 后，可以直接从源码目录或 Git 地址安装，暂不需要维护
``setup.py``。

Install from Git::

   pip install "tider @ git+https://github.com/ZLotusRain/tider.git"

Install in editable mode for development::

   git clone https://github.com/ZLotusRain/tider.git
   cd tider
   pip install -e .

Optional extras::

   pip install -e ".[curlcffi,playwright,socks,rarfile]"


Why named after Tider
=====================

从网络中获取数据的过程就像潮涨潮落，潮起潮落后岸边留下了我们想要的“数据”。潮水时而温润如玉，时而凶猛无常，
每一位想从网络的海洋中获取数据的用户都是掌握潮汐的人。如果控制不当，既可能连岸边也无法触及，也可能会使海岸决堤，
我们需要仔细斟酌如何掌握这股潮汐之力。


Why Tider
=========

- 既可以选择类 Scrapy 的异步编程方式，也可以按照同步模式进行开发。
- 支持多种并发池；在文件下载或长时间等待型任务中，仍可保持较高吞吐。
- 支持用户按需自定义核心模块，包括下载器、调度器、Broker、Pipeline 等。
- 支持 HTTP/2、指纹模拟、浏览器模拟和 SOCKS 代理。
- 支持分布式部署，可从 AMQP 系列队列中获取数据。
- 支持报警、日志上报、远程控制和多进程节点管理。
- 通过 ``Promise`` 和请求链路设计降低复杂抓取流程中的回调嵌套。


Changelog
=========

See `CHANGELOG.md <CHANGELOG.md>`_ for release notes and version history.


Bug tracker
===========

If you have any suggestions, bug reports, or annoyances please report them to the issue tracker:

https://github.com/ZLotusRain/tider/issues
