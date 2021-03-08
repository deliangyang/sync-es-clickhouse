import argparse
import json
import traceback
from queue import Queue, Empty
from threading import Thread
from urllib.parse import quote

import requests
from retrying import retry

from config import url, headers


class Writer(Thread):
    __slots__ = ('queue', 'size', 'headers', 'url', 'fail_log')

    def __init__(self, queue: Queue, size: int, table: str):
        Thread.__init__(self)
        self.queue = queue
        self.size = size
        self.headers = headers
        query = 'INSERT INTO %s FORMAT JSONEachRow' % table
        self.url = url + quote(query)
        self.fail_log = '/tmp/%s.log' % table

    def run(self) -> None:
        print('%s start sync' % self.name)
        data = []
        while True:
            try:
                _line = self.queue.get(True, timeout=3)
                source = json.loads(_line)
                source['_source']['time'] *= 1000
                data.append(json.dumps(source['_source']))
                if len(data) < self.size:
                    continue
                self.push(data)
                data = []
            except Empty:
                print("empty Queue")
                break
            except Exception as e:
                traceback.print_exc()
                print(e)
                break
        if len(data) > 0:
            self.push(data)

    def push(self, data):
        __data_str = '\n'.join(data)
        try:
            print('%s, sync count: %d' % (self.name, len(data)))
            self.submit(__data_str)
            print('%s sync done, next' % self.name)
        except Exception as e:
            with open(self.fail_log, 'a+') as fw:
                fw.write(__data_str)
            print(e)
            exit(1)

    @retry(stop_max_attempt_number=5)
    def submit(self, data):
        resp = requests.post(self.url, data=data, headers=self.headers)
        print(resp.text)
        assert resp.status_code == 200


class Reader(Thread):
    __slots__ = ('queue', 'filename')

    def __init__(self, filename: str, queue: Queue):
        Thread.__init__(self)
        self.queue = queue
        self.filename = filename

    def run(self) -> None:
        for line in self.read():
            self.queue.put(line)

    def read(self):
        with open(self.filename, 'r') as fr:
            while True:
                _line = fr.readline().replace('\n', '')
                if not _line:
                    break
                yield _line


def args():
    parser = argparse.ArgumentParser(description='Sync ES JSONEachRow into ClickHouse')
    parser.add_argument('file', type=str, help='json filename')
    parser.add_argument('table', type=str, help='ClickHouse table name')
    parser.add_argument('--size', dest='size', type=int, default=5000, help='batch size')
    parser.add_argument('--thread', dest='thread', type=int, default=5, help='thread num')

    return parser.parse_args()


if __name__ == '__main__':
    args = args()

    q = Queue(10000)
    reader = Reader(args.file, q)
    reader.start()

    threads = []
    for i in range(args.thread):
        c = Writer(q, args.size, args.table)
        c.start()
        threads.append(c)

    for t in threads:
        t.join()
