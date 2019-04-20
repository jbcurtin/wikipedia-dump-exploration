#!/usr/env/bin python

# import logging
# 
# import bz2
# import hashlib
# import json
# import multiprocessing
# import os
# import redis
# import typing
# 
# import io
# import requests
# import xml.sax
# import xmltodict 
# import xml.etree.ElementTree as etree
# 
# from datetime import datetime
# 
# XSD_STRUCTURES: typing.Dict[str, typing.Any] = {}
# CACHE_DIR: str = '/tmp/etl-cache'
# DEBUG: bool = True
# EVENT_LOOP: typing.Any = None
# WIKI_DIRECTORY: str = 'en-wiki'
# REDIS_HOST: str = '192.168.4.10'
# REDIS_PORT: int = 6379
# REDIS_DB: int = 6
# DATETIME_FORMAT: str = '%Y-%m-%dT%H:%M:%SZ'
# 
# logger = logging.getLogger(__name__)
# 
# if not os.path.exists(CACHE_DIR):
#   os.makedirs(CACHE_DIR)
# 
# class QueuePacker(json.JSONEncoder):
#   def default(self, obj):
#     if isinstance(obj, datetime):
#       return obj.strftime(DATETIME_FORMAT)
# 
#     return super(QueuePacker, self).default(obj)
# 
# class Queue:
#   @staticmethod
#   def Pack(datum: typing.Dict[str, typing.Any]) -> str:
#     return json.dumps(datum, cls=QueuePacker)
# 
#   @staticmethod
#   def UnPack(datum: str) -> typing.Dict[str, typing.Any]:
#     return json.loads(datum)
# 
#   def __init__(self, redis_key):
#     self._key = redis_key
#     self._redis_client = None
# 
#   def flushdb(self) -> None:
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     self._redis_client.flushdb()
# 
#   def size(self) -> int:
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     return int(self._redis_client.llen(self._key))
# 
#   def get(self) -> str:
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     try:
#       return self._redis_client.lpop(self._key).decode('utf-8')
#     except AttributeError:
#       return 'STOP'
# 
#   def put(self, value: str) -> None:
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     self._redis_client.rpush(self._key, value.encode('utf-8'))
# 
#   def bulk(self, stop_iter_key: str, bulk_count: int=50000) -> typing.List[str]:
#     if self._redis_client is None:
#       self._redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
# 
#     index: int = 0
#     datums: typing.List[str] = []
#     while index < bulk_count:
#       datum: str = self._redis_client.lpop(self._key)
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
# def find_in_elem(elem, *args) -> str:
#   base = elem
#   for key in args:
#     try:
#       base = base.find(fix_tag(key))
#     except Exception as err:
#       return 0
# 
#   else:
#     if base == None:
#       return 0
# 
#     elif base.text == None:
#       return 0
# 
#     elif isinstance(base.text, bytes):
#       return base.text.decode('utf-8')
# 
#     elif isinstance(base.text, str):
#       return base.text
# 
#     else:
#       import ipdb; ipdb.set_trace()
#       return 0
# 
# def parse_timestamp(input_datetime: str) -> datetime:
#   if input_datetime == 0:
#     return None
# 
#   return datetime.strptime(input_datetime, '%Y-%m-%dT%H:%M:%SZ')
# 
# def fix_schema_tag(tag) -> str:
#   return '{http://www.w3.org/2001/XMLSchema}%s' % tag
# 
# def fix_tag(tag) -> str:
#   return '{http://www.mediawiki.org/xml/export-0.10/}%s' % tag
# 
# def _decompress_data(work_key: str, done_key: str) -> None:
#   work_queue: Queue = Queue(work_key)
#   done_queue: Queue = Queue(done_key)
#   ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
#   for work_datum in iter(work_queue.get, 'STOP'):
#     work_datum: typing.Dict[str, typing.Any] = Queue.UnPack(work_datum)
#     work_datum['namespaces'] = []
#     work_datum['pages'] = []
#     ologger.info(f'Processing File[{work_datum["filepath"]}]')
#     compressed_stream = bz2.open(work_datum['filepath'], 'r')
#     schema_location: str = None
#     for event, elem in etree.iterparse(compressed_stream, events=('start', 'end')):
#       try:
#         tag_strip: str = '{%s}' % 'http://www.mediawiki.org/xml/export-0.10/'
#         clean_tag: str = elem.tag.split(tag_strip)[1]
#       except (IndexError, KeyError, AttributeError) as err:
#         tag_strip: str = None
#         clean_tag: str = None
# 
#       if event == 'start':
#         if elem.tag in [
#             fix_tag('page'),
#           ]:
#           datum = {
#             'revision_id': int(find_in_elem(elem, 'revision', 'id')),
#             'revision_parentid': int(find_in_elem(elem, 'revision', 'parentid')),
#             'revision_contributor_username': find_in_elem(elem, 'revision', 'contributor', 'username'),
#             'revision_contributor_ip': find_in_elem(elem, 'revision', 'contributor', 'ip'),
#             'revision_contributor_id': int(find_in_elem(elem, 'revision', 'contributor', 'id')),
#             'revision_comment': find_in_elem(elem, 'revision', 'comment'),
#             'revision_timestamp': parse_timestamp(find_in_elem(elem, 'revision', 'timestamp')),
#             'revision_format': find_in_elem(elem, 'revision', 'format'),
#             'revision_sha1': find_in_elem(elem, 'revision', 'sha1'),
#             'revision_text': find_in_elem(elem, 'revision', 'text'),
#             'page_id': find_in_elem(elem, 'id'),
#             'page_title': find_in_elem(elem, 'title'),
#             'page_ns': find_in_elem(elem, 'ns'),
#             'page_redirect': None,
#             'siteinfo': work_datum['siteinfo'],
#             'source': work_datum['filepath'],
#           }
#           if elem.find('redirect'):
#             import ipdb; ipdb.set_trace()
#             pass
# 
#           if datum['page_id'] == 0:
#             continue
# 
#           done_queue.put(Queue.Pack(datum))
# 
#         
#         elif fix_tag('siteinfo') == elem.tag:
#           datum: typing.Dict[str, typing.Any] = {}
#           for child in elem.getchildren():
#             tag_strip: str = '{%s}' % 'http://www.mediawiki.org/xml/export-0.10/'
#             clean_tag: str = child.tag.split(tag_strip)[1]
#             if fix_tag('namespaces') == child.tag:
#               datum['namespaces'] = [ns.text.lower().strip(' ') for ns in child.getchildren() if ns.text]
# 
#             elif child.tag in [
#                 fix_tag('base'),
#                 fix_tag('generator'),
#                 fix_tag('case'),
#                 fix_tag('dbname'),
#                 fix_tag('sitename')]:
#                 datum[clean_tag] = child.text
# 
#           work_datum['siteinfo'] = datum
# 
#         elif elem.tag in [
#             fix_tag('mediawiki'),
#             fix_tag('sitename'),
#             fix_tag('dbname'),
#             fix_tag('case'),
#             fix_tag('generator'),
#             fix_tag('base'),
#             fix_tag('namespaces'),
#             fix_tag('namespace'),
#             fix_tag('title'),
#             fix_tag('ns'),
#             fix_tag('id'),
#             fix_tag('redirect'),
#             fix_tag('revision'),
#             fix_tag('parentid'),
#             fix_tag('timestamp'),
#             fix_tag('contributor'),
#             fix_tag('username'),
#             fix_tag('comment'),
#             fix_tag('model'),
#             fix_tag('format'),
#             fix_tag('text'),
#             fix_tag('sha1'),
#             fix_tag('minor'),
#             fix_tag('ip'),
#           ]:
#           continue
# 
#         else:
#           print(elem.tag)
#           import ipdb; ipdb.set_trace()
#           import sys; sys.exit(1)
# 
#       elif event == 'end':
#         if elem.tag in [
#             fix_tag('namespaces'),
#             fix_tag('page'),
#             fix_tag('siteinfo'),
#         ]:
#           continue
# 
#     import ipdb; ipdb.set_trace()
#     import sys; sys.exit(1)
# 
# def _load_data(work_key: str, done_key: str) -> None:
#   work_queue: Queue = Queue(work_key)
#   done_queue: Queue = Queue(done_key)
#   ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
#   import ipdb ;ipdb.set_trace()
#   import sys; sys.exit(1)
# 
# 
# workers: int = 4
# work_queue: Queue = Queue('work-one')
# work_queue.flushdb()
# processes: typing.List[multiprocessing.Process] = []
# options: typing.Dict[str, typing.Any] = {}
# start: str = os.path.join(os.getcwd(), WIKI_DIRECTORY)
# for root, dirs, files in os.walk(start):
#   for filepath in files:
#     if not filepath.endswith('bz2'):
#       continue
# 
#     work_queue.put(Queue.Pack({
#       'filepath': os.path.join(root, filepath)
#     }))
# 
# for _func, work_key, done_key in [
#     (_decompress_data, 'work-one', 'work-two'),
#     (_load_data, 'work-two', 'work-three')]:
# 
#   if DEBUG:
#     _func(work_key, done_key)
# 
#   else:
#     logger.info(f'Running Function[{_func}]')
#     logger.info(f'Spawned[{workers}] Workers')
#     for worker in range(0, workers):
#       proc: multiprocessing.Process = multiprocessing.Process(target=_func, args=(work_key, done_key))
#       proc.daemon = True
#       proc.start()
#       processes.append(proc)
#       work_queue.put('STOP')
# 
#     work_queue = Queue(work_key)
#     while True:
#       logger.info(f'Queue Size[{work_queue.size()}]')
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
#   # asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
#   # EVENT_LOOP = asyncio.get_event_loop()
#   # EVENT_LOOP.run_until_complete(main(options))
# 
