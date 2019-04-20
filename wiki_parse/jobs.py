#!/usr/env/bin python

from wiki_parse import binding, constants

@binding.follow('noop', pipeline_type=constants.PipelineType.BOTTLE)
def seed_wikidata() -> None:
  import hashlib
  import logging
  import json
  import multiprocessing
  import os
  import requests

  from bs4 import BeautifulSoup
  from datetime import datetime
  from wiki_parse import utils, constants
  work_queue = utils.Queue(seed_wikidata.work_key)
  done_queue = utils.Queue(seed_wikidata.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
  for osi_key in ['en', 'ja']:
    if work_queue.peg(osi_key):
      dump_url: str = f'{constants.DUMP_BASE_URL}/{osi_key}wiki/'
      ologger.info(f'Sourcing URL[{dump_url}]')
      response = requests.get(dump_url, headers=constants.HEADERS, timeout=10)
      soup = BeautifulSoup(response.content, 'html.parser')
      dump_urls = [
          '%s%sdumpstatus.json' % (dump_url, anchor.attrs["href"])
          # f'{work_datum["dump-url"]}{anchor.attrs["href"]}dumpstatus.json'
          for anchor in soup.findAll('a')][1:-1]

      def _sort_dump_urls(alpha):
        date_stamp: str = alpha.rsplit('/', 2)[1]
        date_stamp: datetime = datetime.strptime(date_stamp, '%Y%m%d')
        return date_stamp.timestamp()

      url: str = sorted(dump_urls, key=_sort_dump_urls, reverse=True)[1]
      response = requests.get(url, headers=constants.HEADERS)
      if response.status_code != 200:
        ologger.info(f'File does not exists[{url}]')
        return None

      for table_name, entry in response.json()['jobs'].items():
        if entry['status'] == 'done':
          for filename, file_meta in entry['files'].items():
            if not filename.endswith('bz2'):
              continue

            done_queue.put(utils.Queue.Pack({
              'url': f'{constants.DUMP_BASE_URL}{file_meta["url"]}',
              'filename': filename,
              'sha1': file_meta['sha1'],
              'size': file_meta['size'],
              'table_name': table_name,
              'site': osi_key,
              'output_path': os.path.join(constants.WIKI_DUMP_DIR, filename)
            }))

  import sys; sys.exit(0)

@binding.follow(seed_wikidata, pipeline_type=constants.PipelineType.MULTI, workers=3)
def dump_wikidata():
  import hashlib
  import logging
  import json
  import multiprocessing
  import os
  import requests

  from bs4 import BeautifulSoup
  from datetime import datetime
  from wiki_parse import utils, constants

  work_queue = utils.Queue(dump_wikidata.work_key)
  done_queue = utils.Queue(dump_wikidata.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
  for dump_file in work_queue:
    dump_file = utils.Queue.UnPack(dump_file)
    if not os.path.exists(dump_file['output_path']):
      dump_response = requests.get(dump_file['url'], headers=constants.HEADERS, stream=True)
      local_hash: str = hashlib.sha1()
      file_size: int = 0
      ologger.info(f'Dumping File[{dump_file["filename"]}], file[{dump_file["size"]}]')
      with open(dump_file['output_path'], 'wb') as stream:
        for chunk in dump_response.iter_content(chunk_size=1024):
          local_hash.update(chunk)
          stream.write(chunk)

        else:
          file_size = stream.tell()

      local_hash: str = local_hash.hexdigest()
      if local_hash != dump_file['sha1']:
        ologger.info(f'File Hash[{dump_file["sha1"]}] does not match local_hash[{local_hash}]')
        utils.zero_file(dump_file['output_path'], file_size)

      if file_size != dump_file['size']:
        ologger.info(f'File Size[{dump_file["size"]}] does not match local_hash[{local_hash}]')
        utils.zero_file(dump_file['output_path'], file_size)

@binding.follow(dump_wikidata, pipeline_type=constants.PipelineType.BOTTLE)
def build_converter() -> None:
  import git
  import logging
  import multiprocessing
  import os
  import uuid

  from wiki_parse import utils

  work_queue: utils.Queue = utils.Queue(build_converter.work_key)
  done_queue: utils.Queue = utils.Queue(build_converter.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))

  GIT_URI: str = 'https://github.com/jbcurtin/MWDumper-docker.git'
  DOCKER_TAG: str = 'mwdumper'
  DOCKER_VERSION: str = 'latest'

  if not os.path.exists(convert_files.build_dir):
    git.Repo.clone_from(GIT_URI, convert_files.build_dir, branch='master')

  dockerfile: str = os.path.join(convert_files.build_dir, 'Dockerfile')
  utils.build_dockerfile(convert_files.build_dir, dockerfile, DOCKER_TAG, DOCKER_VERSION)

@binding.follow(build_converter, pipeline_type=constants.PipelineType.MULTI)
def convert_files() -> None:
  import docker
  import logging
  import multiprocessing
  import os

  from wiki_parse import utils, constants

  work_queue: utils.Queue = utils.Queue(build_converter.work_key)
  done_queue: utils.Queue = utils.Queue(build_converter.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))

  filenames: typing.List[typing.Tuple] =[]
  # filename, source_path, output_path
  for root, dirs, files in os.walk(constants.DUMP_DIR):
    filenames.extend(files)

  if not os.path.exists(constants.SQL_DIR):
    os.makedirs(constants.SQL_DIR)

  dump_mount_path: str = os.path.join(os.getcwd(), constants.DUMP_DIR)
  sql_mount_path: str = os.path.join(os.getcwd(), constants.SQL_DIR)
  client: docker.DockerClient = docker.DockerClient()
  for filename in filenames:
    docker_cmd: str = f'/app/run.sh {filename}'
    target_path: str = os.path.join(sql_mount_path, f'{filename}.sql')
    if not os.path.exists(target_path):
      ologger.info(f'Converting File[{filename}]')
      try:
        client.containers.run('mwdumper', docker_cmd, volumes={
          dump_mount_path: {'bind': '/dumps', 'mode': 'rw'},
          sql_mount_path: {'bind': '/sql', 'mode': 'rw'},
        })
      except Exception as err:
        ologger.info(f'Skipping File[{filename}]')

  #for filename in filenames:
  #  client.containers.run('mwdumper', f'/app/run.sh {filename}', mounts=[dump_mount, sql_mount])

  import ipdb; ipdb.set_trace()
  import sys; sys.exit(1)

@binding.follow(convert_files, pipeline_type=constants.PipelineType.BOTTLE)
def seed_database_scheme():
  import docker
  import hashlib
  import logging
  import multiprocessing
  import os
  import requests
  import tempfile
  import time
  import uuid

  from wiki_parse import utils, constants

  work_queue: utils.Queue = utils.Queue(seed_database_scheme.work_key)
  done_queue: utils.Queue = utils.Queue(seed_database_scheme.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))


  GIT_URI: str = 'https://github.com/jbcurtin/MWDumper-docker.git'
  PGSQL_IMAGE_NAME: str = 'postgres:11.2'
  PGSQL_CONTAINER_NAME: str = '-'.join([seed_database_scheme.func_space, 'pgsql'])
  PGSQL_CONTAINER_ROOT_PASSWORD: str = 'aoeu'
  PGSQL_CONTAINER_USER: str = 'usercommon'
  PGLOADER_IMAGE_NAME: str = 'dimitri/pgloader'
  db_name: str = hashlib.md5(seed_database_scheme.func_space.encode(constants.ENCODING)).hexdigest()[0:8]
  # db_name: str = hashlib.md5(str(uuid.uuid4()).encode(constants.ENCODING)).hexdigest()[0:8]
  client: docker.DockerClient = docker.DockerClient()
  mw_schema_uri: str = 'https://phab.wmfusercontent.org/file/data/p3dz4xczy4bhypsezk44/PHID-FILE-cmchp2c7hohhzd7byde6/tables.sql'
  mw_schema_path: str = tempfile.NamedTemporaryFile().name
  response = requests.get(mw_schema_uri, headers=constants.HEADERS)
  if response.status_code != 200:
    ologger.critical('Unable to install Schema')
    import sys; sys.exit(1)

  with open(mw_schema_path, 'w', encoding='utf-8') as stream:
    stream.write(response.content.decode(constants.ENCODING))

  if not os.path.exists(constants.PG_DATA):
    os.makedirs(constants.PG_DATA)

  container_names: typing.List[str] = [container.name for container in client.containers.list()]
  if not PGSQL_CONTAINER_NAME in container_names:
    ologger.info('Setting up PostgreSQL container')
    pg_data_mount_path: str = os.path.join(os.getcwd(), constants.PG_DATA)
    client.containers.run(
        PGSQL_IMAGE_NAME,
        ports={5432:5432},
        name=PGSQL_CONTAINER_NAME,
        dns=[constants.DNS],
        auto_remove=True,
        detach=True,
        volumes={
          pg_data_mount_path: {'bind': '/mnt/data', 'mode': 'rw'},
        },
        environment={
          'POSTGRES_PASSWORD': PGSQL_CONTAINER_ROOT_PASSWORD,
          'POSTGRES_USER': PGSQL_CONTAINER_USER,
          'POSTGRES_DB': db_name,
          'PGDATA': '/mnt/data',
        })
    ologger.info('Stalling for 10 seconds to allow init scripts to finish.')
    time.sleep(10)

  pgsql_active_container: typing.Any = [container for container in client.containers.list() if container.name == PGSQL_CONTAINER_NAME][0]
  pgsql_ip_address: str = pgsql_active_container.attrs['NetworkSettings']['IPAddress']
  pg_conn: str = f'PGPASSWORD={PGSQL_CONTAINER_ROOT_PASSWORD} psql -p 5432 -U {PGSQL_CONTAINER_USER} -h {pgsql_ip_address} {db_name}'
  install_pg_schema: str = f'{pg_conn} -f {mw_schema_path}'
  ologger.info(f'Install Postgres Schema')
  utils.run_command(install_pg_schema)
  
  # Setup next operation
  sql_files: typing.List[str] = []
  for root, dirs, files in os.walk(constants.SQL_DIR):
    sql_files.extend([os.path.join(os.getcwd(), constants.SQL_DIR, sql_file)
      for sql_file in files if sql_file.endswith('.sql')])

  for sql_file in sql_files:
    done_queue.put(utils.Queue.Pack({
      'sql-file-path': sql_file,
      'db-password': PGSQL_CONTAINER_ROOT_PASSWORD,
      'db-name': db_name,
      'db-host': pgsql_ip_address,
      'db-user': PGSQL_CONTAINER_USER,
    }))

@binding.follow(seed_database_scheme, pipeline_type=constants.PipelineType.BOTTLE)
def prepare_converted_sqlfiles():
  import logging
  import multiprocessing
  import os

  from wiki_parse import utils

  work_queue: utils.Queue = utils.Queue(prepare_converted_sqlfiles.work_key)
  done_queue: utils.Queue = utils.Queue(prepare_converted_sqlfiles.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))

  # setup for operation
  sql_files: typing.List[str] = []
  for root, dirs, files in os.walk(constants.SQL_DIR):
    sql_files.extend([os.path.join(os.getcwd(), constants.SQL_DIR, sql_file)
      for sql_file in files if sql_file.endswith('.sql')])

  for sql_file in sql_files:
    work_queue.put(utils.Queue.Pack({
      'sql-file-path': sql_file,
      'db-password': 'aoeu',
      'db-user': 'usercommon',
      'db-host': '172.17.0.3',
      'db-name': '5816bd78',
    }))

  for work_datum in work_queue:
    work_datum: typing.Dict[str, typing.Any] = utils.Queue.UnPack(work_datum)
    input_path: str = work_datum['sql-file-path']
    ologger.info(f'Preparing File[{input_path}]')
    input_dir, input_filename = input_path.rsplit('/', 1)
    output_path: str = os.path.join(input_dir, f'{input_filename}.ready')
    work_datum['sql-ready-path'] = output_path
    with open(input_path, 'r', encoding='utf-8') as stream:
      last_tell: int = 0
      with open(output_path, 'w', encoding='utf-8', newline='\n') as output_stream:
        # remove BEGIN;
        begin_line: str = stream.read(1024)
        mended_line: str = ''.join(begin_line.split('BEGIN;', 1))
        output_stream.write(mended_line)
        while True:
          line: str = stream.read(1024 * 8)
          if last_tell == stream.tell():
            break

          last_tell = stream.tell()
          parts: typing.List[str] = [p for p in line.split("\\'") if p]
          whole_line: str = "''".join(parts)
          output_stream.write(whole_line)

      if last_tell == 0:
        continue

    import ipdb ;ipdb.set_trace()
    import sys; sys.exit(1)

@binding.follow(seed_database_scheme, pipeline_type=constants.PipelineType.MULTI)
def load_converted_files():
  import logging
  import multiprocessing
  import os

  from wiki_parse import utils

  work_queue: utils.Queue = utils.Queue(load_converted_files.work_key)
  done_queue: utils.Queue = utils.Queue(load_converted_files.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))

  # # setup for operation
  # sql_files: typing.List[str] = []
  # for root, dirs, files in os.walk(constants.SQL_DIR):
  #   sql_files.extend([os.path.join(os.getcwd(), constants.SQL_DIR, sql_file)
  #     for sql_file in files if sql_file.endswith('.sql')])

  # for sql_file in sql_files:
  #   work_queue.put(utils.Queue.Pack({
  #     'sql-file-path': sql_file,
  #     'db-password': 'aoeu',
  #     'db-user': 'usercommon',
  #     'db-host': '172.17.0.3',
  #     'db-name': '5816bd78',
  #   }))

  for work_datum in work_queue:
    work_datum: typing.Dict[str, str] = utils.Queue.UnPack(work_datum)
    pg_conn: str = f'PGPASSWORD={work_datum["db-password"]} psql -U {work_datum["db-user"]} -h {work_datum["db-host"]} {work_datum["db-name"]}'
    pg_cmd: str = f'{pg_conn} -f {work_datum["sql-file-path"]}'
    ologger.info(f'Importing Sql File[{work_datum["sql-file-path"]}')
    utils.run_command(pg_cmd)
    
@binding.follow(dump_wikidata)
def decompress_files() -> None:
  import bz2
  import logging
  import multiprocessing

  import xml.etree.ElementTree as etree

  from wiki_parse import utils

  work_queue: utils.Queue = utils.Queue(decompress_files.work_key)
  done_queue: utils.Queue = utils.Queue(decompress_files.done_key)
  ologger = logging.getLogger('.'.join([__name__, multiprocessing.current_process().name]))
  for work_datum in iter(work_queue.get, 'STOP'):
    work_datum: typing.Dict[str, typing.Any] = utils.Queue.UnPack(work_datum)
    work_datum['namespaces'] = []
    work_datum['pages'] = []
    ologger.info(f'Processing File[{work_datum["filepath"]}]')
    compressed_stream = bz2.open(work_datum['filepath'], 'r')
    schema_location: str = None
    for event, elem in etree.iterparse(compressed_stream, events=('start', 'end')):
      try:
        tag_strip: str = '{%s}' % 'http://www.mediawiki.org/xml/export-0.10/'
        clean_tag: str = elem.tag.split(tag_strip)[1]
      except (IndexError, KeyError, AttributeError) as err:
        tag_strip: str = None
        clean_tag: str = None

      if event == 'start':
        if elem.tag in [
            utils.fix_tag('page'),
          ]:
          datum = {
            'revision_id': int(utils.find_in_elem(elem, 'revision', 'id')),
            'revision_parentid': int(utils.find_in_elem(elem, 'revision', 'parentid')),
            'revision_contributor_username': utils.find_in_elem(elem, 'revision', 'contributor', 'username'),
            'revision_contributor_ip': utils.find_in_elem(elem, 'revision', 'contributor', 'ip'),
            'revision_contributor_id': int(utils.find_in_elem(elem, 'revision', 'contributor', 'id')),
            'revision_comment': utils.find_in_elem(elem, 'revision', 'comment'),
            'revision_timestamp': utils.parse_timestamp(utils.find_in_elem(elem, 'revision', 'timestamp')),
            'revision_format': utils.find_in_elem(elem, 'revision', 'format'),
            'revision_sha1': utils.find_in_elem(elem, 'revision', 'sha1'),
            'revision_text': utils.find_in_elem(elem, 'revision', 'text'),
            'page_id': utils.find_in_elem(elem, 'id'),
            'page_title': utils.find_in_elem(elem, 'title'),
            'page_ns': utils.find_in_elem(elem, 'ns'),
            'page_redirect': None,
            'siteinfo': work_datum['siteinfo'],
            'source': work_datum['filepath'],
          }
          if elem.find('redirect'):
            import ipdb; ipdb.set_trace()
            pass

          if datum['page_id'] == 0:
            continue

          done_queue.put(utils.Queue.Pack(datum))

        
        elif utils.fix_tag('siteinfo') == elem.tag:
          datum: typing.Dict[str, typing.Any] = {}
          for child in elem.getchildren():
            tag_strip: str = '{%s}' % 'http://www.mediawiki.org/xml/export-0.10/'
            clean_tag: str = child.tag.split(tag_strip)[1]
            if utils.fix_tag('namespaces') == child.tag:
              datum['namespaces'] = [ns.text.lower().strip(' ') for ns in child.getchildren() if ns.text]

            elif child.tag in [
                utils.fix_tag('base'),
                utils.fix_tag('generator'),
                utils.fix_tag('case'),
                utils.fix_tag('dbname'),
                utils.fix_tag('sitename')]:
                datum[clean_tag] = child.text

          work_datum['siteinfo'] = datum

        elif elem.tag in [
            utils.fix_tag('mediawiki'),
            utils.fix_tag('sitename'),
            utils.fix_tag('dbname'),
            utils.fix_tag('case'),
            utils.fix_tag('generator'),
            utils.fix_tag('base'),
            utils.fix_tag('namespaces'),
            utils.fix_tag('namespace'),
            utils.fix_tag('title'),
            utils.fix_tag('ns'),
            utils.fix_tag('id'),
            utils.fix_tag('redirect'),
            utils.fix_tag('revision'),
            utils.fix_tag('parentid'),
            utils.fix_tag('timestamp'),
            utils.fix_tag('contributor'),
            utils.fix_tag('username'),
            utils.fix_tag('comment'),
            utils.fix_tag('model'),
            utils.fix_tag('format'),
            utils.fix_tag('text'),
            utils.fix_tag('sha1'),
            utils.fix_tag('minor'),
            utils.fix_tag('ip'),
          ]:
          continue

        else:
          print(elem.tag)
          import ipdb; ipdb.set_trace()
          import sys; sys.exit(1)

      elif event == 'end':
        if elem.tag in [
            utils.fix_tag('namespaces'),
            utils.fix_tag('page'),
            utils.fix_tag('siteinfo'),
        ]:
          continue

    import ipdb; ipdb.set_trace()
    import sys; sys.exit(1)


