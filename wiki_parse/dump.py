#!/usr/bin/env python

# import logging
# import hashlib
# import multiprocessing
# import json
# import os
# import redis
# import requests
# import signal
# import time
# 
# from urllib.parse import urlparse
# 
# logger = logging.getLogger('') # <--- Probable a good idea to name your logger. '' is the 'root' logger
# sysHandler = logging.StreamHandler()
# sysHandler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
# logger.addHandler(sysHandler)
# logger.setLevel(logging.INFO)
# 
# logger = logging.getLogger(__name__)
# 
# OUTPUT_DIR = './wiki-dump'
# REDIS_HOST = '192.168.4.10'
# REDIS_PORT = 6379
# REDIS_DB = 5
# HEADERS = {}
# DEBUG = False if os.environ.get('DEBUG', 'true') in ['f', 'false', 'no'] else True
# 
# logging.info('DEBUG[%s]' % str(DEBUG))
# 
# if not os.path.exists(OUTPUT_DIR):
#   os.makedirs(OUTPUT_DIR)
# 
# class Queue:
#   def __init__(self, redis_key):
#     self._key = redis_key
#     self._redis_client = None
# 
#   def flushdb(self):
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     self._redis_client.flushdb()
# 
#   def size(self):
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     return int(self._redis_client.llen(self._key))
# 
#   def get(self):
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     try:
#       return self._redis_client.lpop(self._key).decode('utf-8')
#     except AttributeError:
#       return 'STOP'
# 
#   def put(self, value):
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     self._redis_client.rpush(self._key, value.encode('utf-8'))
# 
#   def bulk(self, stop_iter_key, bulk_count=50000):
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     index = 0
#     datums = []
#     while index < bulk_count:
#       datum = self._redis_client.lpop(self._key)
#       if datum is None:
#         break
# 
#       if datum == stop_iter_key:
#         break
# 
#       datums.append(datum.decode('utf-8'))
# 
#     logging.info(len(datums))
#     return datums
# 
# workers = 6
# work_queue = Queue('work-one')
# work_queue.flushdb()
# processes = []
# 
# from bs4 import BeautifulSoup
# DUMP_BASE_URL = 'https://dumps.wikimedia.org'
# dump_urls = []
# 
# url = 'https://en.wikipedia.org/wiki/List_of_Wikipedias'
# response = requests.get(url, headers=HEADERS)
# soup = BeautifulSoup(response.content, 'html.parser')
# for osi_key in ['en', 'ja']:
#   work_queue.put(json.dumps({
#     'dump-url': '%s/%swiki/' % (DUMP_BASE_URL, osi_key)
#   }))
# #for row in soup.find('span', {'id': 'List', 'class': 'mw-headline'}).parent.findNextSibling('table', {'class': 'wikitable'}).find('tbody').findAll('tr'):
# #  try:
# #    col = row.findAll('td')[3]
# #  except IndexError:
# #    continue
#   
# #  work_queue.put(json.dumps({
# #    'dump-url': '%s/%swiki/' % (DUMP_BASE_URL, col.text)
# #  }))
# 
# def _map_dump_urls(work_key: str, done_key: Queue) -> None:
#   work_queue = Queue(work_key)
#   done_queue = Queue(done_key)
#   ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
#   for work_datum in iter(work_queue.get, 'STOP'):
#     work_datum = json.loads(work_datum)
#     ologger.info('Sourcing URL[%s]' % work_datum["dump-url"])
#     response = requests.get(work_datum['dump-url'], headers=HEADERS, timeout=10)
#     soup = BeautifulSoup(response.content, 'html.parser')
#     dump_urls = [
#         '%s%sdumpstatus.json' % (work_datum["dump-url"], anchor.attrs["href"])
#         # f'{work_datum["dump-url"]}{anchor.attrs["href"]}dumpstatus.json'
#         for anchor in soup.findAll('a')][1:-1]
# 
#     work_datum['files'] = []
#     for url in dump_urls:
#       ologger.info('Sourcing URL[%s]' % url)
#       response = requests.get(url, headers=HEADERS)
#       if response.status_code != 200:
#         ologger.info('File does not exist[%s]' % url)
#         continue
# 
#       try:
#         items = response.json()['jobs'].items()
#       except Exception:
#         import ipdb; ipdb.set_trace()
#         import sys; sys.exit(1)
# 
#       for key, entry in response.json()['jobs'].items():
#         if entry['status'] in ['done']:
#           for filename, file_item in entry['files'].items():
#             output_path = '%s/%s' % (OUTPUT_DIR, filename)
#             work_datum['files'].append({
#               'md5': file_item.get('md5', None),
#               'sha1': file_item.get('sha1', None),
#               'url': 'https://dumps.wikimedia.org%s' % file_item['url'],
#               'size': file_item['size'],
#               'filename': filename,
#               'output_path': output_path,
#             })
# 
#     done_queue.put(json.dumps(work_datum))
#     # if done_queue.size() > 0:
#     #  break
# 
# def _zero_file(filepath, file_size):
#   with open(filepath, 'wb') as stream:
#     for chunk in iter(lambda: b'\0' * 4096, b''):
#       stream.write(chunk)
#       if stream.tell() > file_size:
#         break
# 
# def _sync_data(work_key, done_key):
#   work_queue = Queue(work_key)
#   done_queue = Queue(done_key)
#   ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
#   for work_datum in iter(work_queue.get, 'STOP'):
#     work_datum = json.loads(work_datum)
#     for _file in work_datum['files']:
#       #if _file['size'] > 1000:
#       #  continue
# 
#       dump_response = requests.get(_file['url'], headers=HEADERS, stream=True)
#       if not os.path.exists(_file['output_path']):
#         ologger.info('Dumping File[%s:%s]' % (_file["filename"], _file["size"]))
# 
#         with open(_file['output_path'], 'wb') as stream:
#           for chunk in dump_response.iter_content(chunk_size=1024):
#             stream.write(chunk)
# 
#         with open(_file['output_path'], 'rb') as stream:
#           local_hash = hashlib.md5()
#           for chunk in iter(lambda: stream.read(4096), b''):
#             local_hash.update(chunk)
# 
#           local_hash = local_hash.hexdigest()
#           file_size = stream.tell()
# 
#         if local_hash != _file['md5']:
#           ologger.error('MD5[{_file["md5"]}:{local_hash}] Does not match Downloaded File[{output_path}]')
#           _zero_file(_file['output_path'], file_size)
# 
#         if file_size != _file['size']:
#           ologger.error('Size[{_file["size"]}] Dose not match Downloaded File[{output_path}]')
#           _zero_file(_file['output_path'], file_size)
# 
#       else:
#         ologger.info('Skipping File[{_file["filename"] %s}]' % _file["filename"])
# 
# for _func, work_key, done_key in [
#     (_map_dump_urls, 'work-one', 'work-two'),
#     (_sync_data, 'work-two', 'work-three'),
#   ]:
#   logger.info('Running Function[%s]' % _func)
#   logger.info('Spawned[%s] Workers' % str(workers))
#   if DEBUG:
#     _func(work_key, done_key)
# 
#   else:
#     for worker in range(0, workers):
#       proc = multiprocessing.Process(target=_func, args=(work_key, done_key))
#       proc.daemon = True
#       proc.start()
#       processes.append(proc)
#       work_queue.put('STOP')
# 
#     work_queue = Queue(work_key)
#     while True:
#       logger.info('Queue Size[%s]' % work_queue.size())
#       if work_queue.size() == 0:
#         for proc in processes:
#           if proc.is_alive():
#             os.kill(proc.pid, signal.SIGKILL)
# 
#         else:
#           break
# 
#       time.sleep(5)
# 
# import sys; sys.exit(1)
# 
