#!/usr/env/bin python

import logging


import bz2
import docker
import hashlib
import json
import multiprocessing
import os
import redis
import subprocess
import time
import typing

import requests

from datetime import datetime

from wiki_parse import constants

logger = logging.getLogger(__name__)

class QueuePacker(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, datetime):
      return obj.strftime(constants.DATETIME_FORMAT)

    return super(QueuePacker, self).default(obj)

class Queue:
  DEFAULT_DELAY: int = 1728000
  @staticmethod
  def Pack(datum: typing.Dict[str, typing.Any]) -> str:
    return json.dumps(datum, cls=QueuePacker)

  @staticmethod
  def UnPack(datum: str) -> typing.Dict[str, typing.Any]:
    return json.loads(datum)

  def __init__(self, redis_key):
    self._key = redis_key
    self._redis_client = None

  def __iter__(self):
    return self

  def __next__(self):
    value = self.get()
    if value is None or value == 'STOP':
      raise StopIteration

    return value

  def peg(self, key: str, delay: str=DEFAULT_DELAY) -> bool:
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=constants.REDIS_DB)

    value: str = self._redis_client.get(key)
    if value is None:
      self._redis_client.set(key, 1)
      self._redis_client.expire(key, delay)
      return True

    return False

  def flushdb(self) -> None:
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=constants.REDIS_DB)

    self._redis_client.flushdb()

  def put_block(self, value: str, delay: int=1728000) -> None:
    value_sorted = ''.join(sorted(value))
    value_sorted = ''.join(['put-block', value_sorted])
    value_hash = hashlib.md5(value_sorted.encode('utf-8')).hexdigest()
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=constants.REDIS_DB)

    if self._redis_client.get(value_hash) is None:
      self._redis_client.set(value_hash, 1)
      self._redis_client.expire(value_hash, delay)
      self.put(value)

  def size(self) -> int:
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST, port=constants.REDIS_PORT, db=constants.REDIS_DB)

    return int(self._redis_client.llen(self._key))

  def get(self) -> str:
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST,
          port=constants.REDIS_PORT, db=constants.REDIS_DB)

    try:
      return self._redis_client.lpop(self._key).decode('utf-8')
    except AttributeError:
      return 'STOP'

  def put(self, value: str) -> None:
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST,
          port=constants.REDIS_PORT, db=constants.REDIS_DB)

    self._redis_client.rpush(self._key, value.encode('utf-8'))

  def bulk(self, stop_iter_key: str, bulk_count: int=50000) -> typing.List[str]:
    if self._redis_client is None:
      self._redis_client = redis.Redis(host=constants.REDIS_HOST,
          port=constants.REDIS_PORT, db=constants.REDIS_DB)

    index: int = 0
    datums: typing.List[str] = []
    while index < bulk_count:
      datum: str = self._redis_client.lpop(self._key)
      if datum is None:
        break

      if datum == stop_iter_key:
        break

      datums.append(datum.decode('utf-8'))

    logging.info(len(datums))
    return datums

def find_in_elem(elem, *args) -> str:
  base = elem
  for key in args:
    try:
      base = base.find(fix_tag(key))
    except Exception as err:
      return 0

  else:
    if base == None:
      return 0

    elif base.text == None:
      return 0

    elif isinstance(base.text, bytes):
      return base.text.decode('utf-8')

    elif isinstance(base.text, str):
      return base.text

    else:
      import ipdb; ipdb.set_trace()
      return 0

def parse_timestamp(input_datetime: str) -> datetime:
  if input_datetime == 0:
    return None

  return datetime.strptime(input_datetime, '%Y-%m-%dT%H:%M:%SZ')

def fix_schema_tag(tag) -> str:
  return '{http://www.w3.org/2001/XMLSchema}%s' % tag

def fix_tag(tag) -> str:
  return '{http://www.mediawiki.org/xml/export-0.10/}%s' % tag

def zero_file(filepath, file_size):
  with open(filepath, 'wb') as stream:
    for chunk in iter(lambda: b'\0' * 4096, b''):
      stream.write(chunk)
      if stream.tell() > file_size:
        break

def run_command(cmd: str, allow_error: int = 0) -> str:
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
  cmd: typing.List[str] = cmd.split(' ')
  proc = subprocess.Popen(' '.join(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
  while proc.poll() is None:
    time.sleep(.1)

  if proc.poll() > 0:
    if allow_error != proc.poll():
      raise NotImplementedError(f'{proc.poll()}, {proc.stderr.read()}')

  return proc.stdout.read().decode('utf-8')

def build_dockerfile(dockerfile_dir: str, build_filename: str, build_name: str, version: str='latest') -> typing.Tuple[docker.DockerClient, str]:
  client = docker.DockerClient()
  dockerfile_path = os.path.join(dockerfile_dir, build_filename)
  if not os.path.exists(dockerfile_path):
    raise IOError(f'Missing Dockerfile[{dockerfile_path}]')

  docker_tags: typing.List[str] = [item for sublist in [image.tags for image in client.images.list()] for item in sublist]
  docker_tag = f'{build_name}:{version}'
  if not docker_tag in docker_tags \
      or constants.DOCKER_NO_CACHE:
    ologger = logging.getLogger('.'.join([__name__,'build_dockerfile', multiprocessing.current_process().name]))
    ologger.info(f'Building Docker Image[{docker_tag}]')
    client.images.build(
        nocache=constants.DOCKER_NO_CACHE,
        path=dockerfile_dir,
        dockerfile=dockerfile_path,
        tag=docker_tag)

