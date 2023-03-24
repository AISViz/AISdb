#!/bin/bash
set -e

SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "${SCRIPTPATH}/.."
TAG=`cat pyproject.toml | grep version | grep -Eo --color=no "[[:digit:]]+\.[[:digit:]]+\.[[:digit:]]+"`

echo "GIT CONTEXT:"
echo
git ls-files --exclude-from=.gitignore

echo
echo
echo "CAUTION: TAGGING PUBLIC IMAGE $TAG WITH BUILD CONTEXT:"
echo
git ls-files --exclude-from=.dockerignore
echo

read -p "Are you sure? [y/n]" -r

if [[ $REPLY =~ ^[Yy]$ ]]
then
  pwd
  sudo -E docker-compose build aisdb-python python-test
  docker tag meridiancfi/aisdb:latest meridiancfi/aisdb:$TAG 
  docker tag meridiancfi/aisdb-manylinux:latest meridiancfi/aisdb-manylinux:latest
  sudo docker push meridiancfi/aisdb:$TAG
  sudo docker push meridiancfi/aisdb:latest
  sudo docker push meridiancfi/aisdb-manylinux:latest
fi

