import time
import os
import psutil
import openpyxl
import matplotlib.pyplot as plt
from multiprocessing import Process, Event

from .time import preferred_clock


class MemoryGraph:

    def __init__(self, unit='MB', interval=1, path='/', keep_data=True):
        if unit not in ('B', 'KB', 'MB', 'GB'):
            raise ValueError('Unsupported memory unit: {}'.format(unit))
        self._unit = unit  # B|KB|MB|GB
        self._interval = interval
        self._path = path
        self._keep_data = keep_data
        self._start_time = None
        self.elapsed = None

        self._pid = os.getpid()
        self._task = Process(target=self._run)

        self._is_stopped = Event()

    def start(self):
        self._start_time = preferred_clock()
        self._task.start()

    @property
    def denominator(self):
        if self._unit == 'B':
            return 1
        elif self._unit == 'KB':
            return 1024
        elif self._unit == 'MB':
            return 1024 * 1024
        return 1024 * 1024 * 1024

    def _run(self):
        memory_usage = {}
        process = psutil.Process(self._pid)
        count = 0
        while not self._is_stopped.is_set():
            mem_info = process.memory_info()
            rss = round(mem_info.rss / self.denominator, 2)
            memory_usage[count] = rss
            time.sleep(self._interval)
            count += 1
        mem_info = process.memory_info()
        rss = round(mem_info.rss / self.denominator, 2)
        memory_usage[count] = rss

        plt.figure(figsize=(10, 5))
        plt.plot(list(memory_usage.values()), label=f'Memory Usage ({self._unit})')
        plt.xlabel('Time')
        plt.ylabel(f'Memory Usage ({self._unit})')
        plt.title('Memory Usage Over Time')
        plt.legend()
        plt.savefig(os.path.join(self._path, f'memory_usage_{self._pid}.png'))

        if not self._keep_data:
            return
        head = ('', 'rss')
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(head)
        for t, rss in memory_usage.items():
            ws.append([t, rss])
        wb.save(os.path.join(self._path, f'memory_usage_{self._pid}.xlsx'))
        wb.close()

    def stop(self):
        self.elapsed = preferred_clock() - self._start_time
        self._is_stopped.set()
        self._task.join()

    def __enter__(self):
        self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
