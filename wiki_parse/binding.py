import functools
import hashlib
import multiprocessing
import os
import types
import typing

from wiki_parse import constants

DAISY_CHAIN = {}
ENCODING = 'utf-8'

# Bert, a microframework for simple ETL solutions
def follow(parent_func: typing.Union[str, types.FunctionType], pipeline_type: constants.PipelineType = constants.PipelineType.BOTTLE, workers: int = multiprocessing.cpu_count()):
  if isinstance(parent_func, str):
    parent_func_space = hashlib.sha1(parent_func.encode(ENCODING)).hexdigest()
    parent_func_work_key = hashlib.sha1(''.join([parent_func_space, 'work']).encode(ENCODING)).hexdigest()
    parent_func_done_key = hashlib.sha1(''.join([parent_func_space, 'done']).encode(ENCODING)).hexdigest()

  elif isinstance(parent_func, types.FunctionType):
    parent_func_space = hashlib.sha1(parent_func.__name__.encode(ENCODING)).hexdigest()
    parent_func_work_key = hashlib.sha1(''.join([parent_func_space, 'work']).encode(ENCODING)).hexdigest()
    parent_func_done_key = hashlib.sha1(''.join([parent_func_space, 'done']).encode(ENCODING)).hexdigest()
    if getattr(parent_func, 'work_key', None) is None:
      parent_func.work_key = parent_func_work_key

    if getattr(parent_func, 'done_key', None) is None:
      parent_func.done_key = parent_func_done_key

  else:
    raise NotImplementedError


  @functools.wraps(parent_func)
  def _parent_wrapper(wrapped_func):
    wrapped_func_space: str = hashlib.sha1(
        wrapped_func.__name__.encode(ENCODING)).hexdigest()
    wrapped_func_work_key: str = parent_func_done_key
    wrapped_func_done_key: str = hashlib.sha1(
        ''.join([wrapped_func_space, 'done']).encode(ENCODING)).hexdigest()
    wrapped_func_build_dir: str = os.path.join('/tmp', wrapped_func_space, 'build')

    if getattr(wrapped_func, 'func_space', None) is None:
      wrapped_func.func_space = wrapped_func_space

    if getattr(wrapped_func, 'work_key', None) is None:
      wrapped_func.work_key = wrapped_func_work_key

    if getattr(wrapped_func, 'done_key', None) is None:
      wrapped_func.done_key = wrapped_func_done_key

    if getattr(wrapped_func, 'build_dir', None) is None:
      wrapped_func.build_dir = wrapped_func_build_dir


    chain: typing.List[types.FunctionType] = DAISY_CHAIN.get(parent_func_space, [])
    chain.append(wrapped_func_space)
    DAISY_CHAIN[parent_func_space] = chain

    @functools.wraps(wrapped_func)
    def _wrapper(*args, **kwargs):
      return wrapped_func(*args, **kwargs)

    return _wrapper
  return _parent_wrapper

def bind_jobs():
  pass


