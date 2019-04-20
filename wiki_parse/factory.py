#!/usr/env/bin python

import argparse
import importlib
import logging
import multiprocessing
import os
import signal
import time
import typing
import types

from datetime import datetime

from wiki_parse import constants, utils

logger = logging.getLogger(__name__)

JOBS: typing.Dict[str, types.FunctionType] = {}
STOP_DAEMON: bool = False

def capture_options() -> typing.Any:
  parser = argparse.ArgumentParser()
  parser.add_argument('-j', '--jobs', default=None, required=True)
  parser.add_argument('-d', '--debug', action='store_false', default=True)
  # parser.add_argument('-w', '--work-key', default='work-one')
  # parser.add_argument('-v', '--done-key', default='work-two')
  return parser.parse_args()

def setup(options) -> None:
  if not os.path.exists(constants.CACHE_DIR):
    os.makedirs(constants.CACHE_DIR)

  if not os.path.exists(constants.WIKI_DUMP_DIR):
    os.makedirs(constants.WIKI_DUMP_DIR)

def scan_jobs(options):
  global JOBS
  module = importlib.import_module('wiki_parse.jobs')
  for member_name in dir(module):
    if member_name.startswith('_'):
      continue

    member = getattr(module, member_name)
    if type(member) != types.FunctionType:
      continue

    JOBS[member_name] = member

def validate_jobs(options):
  for job in options.jobs.split(','):
    if job not in JOBS.keys():
      raise NotImplementedError(f'Job[{job}] not found. Available Jobs[{", ".join(JOBS.keys())}]')

def handle_signal(sig, frame):
  if sig == 2:
    global STOP_DAEMON
    STOP_DAEMON = True
    import sys; sys.exit(0)

  else:
    logger.info('Unhandled Signal[{sig}]')

def start_jobs(options):
  signal.signal(signal.SIGINT, handle_signal)
  while not STOP_DAEMON:
    if constants.DEBUG:
      for job in options.jobs.split(','):
        logger.info(f'Running Job[{job}]')
        JOBS[job]()

      time.sleep(constants.DELAY)
      break
    else:
      # Rewrite this part, so that binding.follow specifies how many parallel may exist
      processes: typing.List[multiprocessing.Process] = []
      for idx in range(0, constants.WORKERS):
        logging.info(f'Spawning Process[{idx}]')
        jobs: typing.List[str] = options.jobs.split(',')
        next_func = idx % len(jobs)
        next_job = sorted(jobs)[next_func]
        proc: multiprocessing.Process = multiprocessing.Process(target=JOBS[next_job])
        proc.daemon = True
        proc.start()
        processes.append(proc)
        time.sleep(1)

      else:
        while not STOP_DAEMON:
          time.sleep(constants.DELAY)

        for proc in processes:
          if proc.is_alive():
            os.kill(proc.pid, signal.SIGKILL)

        else:
          break

if __name__ in ['__main__']:
  options = capture_options()
  setup(options)
  scan_jobs(options)
  validate_jobs(options)
  start_jobs(options)

