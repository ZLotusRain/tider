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
:Version: 2.0.1
:Source: https://github.com/ZLotusRain/tider


Overview
========

Tider是一个适合国内爬虫开发生态的，拥有极高性能和潜力的新一代的网路爬虫框架（web crawling and scraping framework）


Requirements
============
- Python 3.7+
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
爬虫的过程就像潮汐一样，携带着原始信息，冲上海岸，当潮水退去时，岸边留下的就是想要的数据。潮水有时温柔，
有时凶猛，如果控制不当，可能会冲毁目标海岸，也可能连岸边也无法触及，每个爬虫用户都是掌握潮汐的人，需要
仔细斟酌上岸的方法。

Why Tider
=========

- 其他的爬虫框架
   - scrapy: 太重了，而且其请求相关的逻辑不符合项目开发
   - autoscraper: 以urllib3作为底层请求库，一些请求操作不方便；扩展性差
   - feapder: 扩展性不太行，调度完全基于redis，限制比较多，性能不够强，且其还存在收费项目
- Tider
   - 既可以选择以scrapy的方式编写爬虫，也可以按照python本身的模式进行开发
   - 支持多种并发池，在进行文件下载或执行需要长时间等待的任务时，并发池威力不减，用户可以根据需要选择适合具体场景的并发池， 推荐使用gevent
   - 速度快且支持控制，以10000次百度请求任务为例，tider的速度比feapder和scrapy快3-5倍
   - 支持用户按需自定核心模块，甚至可更改底层的请求库
   - 天生避免回调地狱，这一点就秒杀了所有爬虫框架
   - 支持类似celery的分布式
   - 支持报警监控
   - 支持远程控制


Bug tracker
===========
| If you have any suggestions, bug reports, or annoyances please report them to my issue tracker at 
| https://github.com/ZLotusRain/tider/issues

