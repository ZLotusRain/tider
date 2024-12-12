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
:Version: 3.2.0
:Source: https://github.com/ZLotusRain/tider


Overview
========

Tider是新一代的网路爬虫框架（web crawling and scraping framework），集高性能与高拓展性于一身，拥有强大的分布式任务处理能力，
支持多种请求下载器，如socket通信、浏览器模拟等，支持HTTP/2协议，支持任务的远程监控。


Requirements
============
- Python 3.7+ and Python 3.9+ is more recommended
- Works on Linux, Windows, macOS


Installation
============
| 目前Tider暂未发布到pypi，所以没法通过pip等包管理工具来安装，可以通过``git clone``的方式使用

- Clone with SSH
   git@github.com:ZLotusRain/tider.git
- Clone with HTTP
   https://github.com/ZLotusRain/tider.git


Why named after Tider
=====================
从网络中获取数据的过程就像潮涨潮落，潮起潮落后岸边留下了我们想要的“数据“。潮水时而温润如玉，时而凶猛无常，
每一位想从网络的海洋中获取数据的用户都是掌握潮汐的人，如果控制不当，既可能连岸边也无法触及，也可能会使海岸决堤，
我们需要仔细斟酌如何掌握这股潮汐之力。

Why Tider
=========

- 其他的爬虫框架
   - scrapy: 太重了，而且其请求相关的逻辑不符合项目开发
   - autoscraper: 以urllib3作为底层请求库，一些请求操作不方便；扩展性差
   - feapder: 扩展性不太行，调度完全基于redis，限制比较多，性能不够强
- Tider
   - 既可以选择类scrapy的异步编程方式（推荐使用该方式），也可以按照同步模式进行开发
   - 支持多种并发池，在进行文件下载或执行需要长时间等待的任务时，并发池威力不减，用户可以根据需要选择适合具体场景的并发池，推荐使用gevent
   - 速度快，以线程池为例，其请求速度与直接使用Python中的`concurrent.futures.ThreadPoolExecutor`进行简单的并发配置的速度相当
   - 支持用户按需自定核心模块，可自定义请求下载器
   - 支持HTTP/2协议，指纹模拟，浏览器模拟
   - 天生避免回调地狱，这一点就秒杀了所有爬虫框架
   - 支持分布式部署，支持从AMQP系列队列中获取数据
   - 支持报警监控
   - 支持远程控制


Bug tracker
===========
| If you have any suggestions, bug reports, or annoyances please report them to my issue tracker at 
| https://github.com/ZLotusRain/tider/issues

