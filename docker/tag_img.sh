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

cd $SCRIPTPATH
read -p "Are you sure? [y/n]" -r

if [[ $REPLY =~ ^[Yy]$ ]]
then
  docker build --target aisdb --tag meridiancfi/aisdb:$TAG --tag meridiancfi/aisdb:latest --compress --file ./Dockerfile .. 
  sudo docker push meridiancfi/aisdb:$TAG
  #docker build --target aisdb --tag meridiancfi/aisdb:latest --compress --file ./Dockerfile .. 
  sudo docker push meridiancfi/aisdb:latest
fi

