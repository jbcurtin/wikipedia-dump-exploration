#!/usr/bin/env bash

set -e
set -x

if [ "$1" == "dump" ];then
  rm -rf wiki-dump
  echo 'flushall'|redis-cli -h 192.168.4.10
  PYTHONPATH='.' python wiki_parse/factory.py -j seed_wikidata
  DUMP_DIR=jawiki-dump OSI_KEY=ja DEBUG=f WORKERS=3 PYTHONPATH='.' python wiki_parse/factory.py -j dump_wikidata
  DUMP_DIR=enwiki-dump OSI_KEY=en DEBUG=f WORKERS=3 PYTHONPATH='.' python wiki_parse/factory.py -j dump_wikidata
fi

if [ "$1" == "convert" ]; then
  git clone git@github.com:Ueland/MWDumper-docker.git || true
  cd MWDumper-docker
  docker build . -t mwdumper
  cd -
  rm -rf MWDumper-docker
  rm -rf enwiki-sql
  mkdir -p enwiki-sql
  docker run -v $PWD/enwiki-dump:/dumps -v $PWD/enwiki-sql:/sql mwdumper
fi

if [ "$1" == 'load-all' ]; then
  source $HOME/.bashrc
  # docker-stop-all
  # docker-remove-all
  # rm -rf en-pgdata
  # echo 'flushall'|redis-cli -h 192.168.4.10
  # PYTHONPATH='.' python wiki_parse/factory.py -j seed_database_scheme
  DEBUG=f WORKERS=4 PYTHONPATH='.' python wiki_parse/factory.py -j load_converted_files
fi
