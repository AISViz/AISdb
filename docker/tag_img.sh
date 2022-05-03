#!/bin/bash
SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
cd "${SCRIPTPATH}/.."
echo "GIT CONTEXT:"
echo
git ls-files --exclude-from=.gitignore
echo
echo
echo "CAUTION: TAGGING PUBLIC IMAGE WITH BUILD CONTEXT:"
echo
git ls-files --exclude-from=.dockerignore
echo
cd $SCRIPTPATH
read -p "Are you sure? [y/n]" -r
if [[ $REPLY =~ ^[Yy]$ ]]
then
  docker build --target webserv --tag meridiancfi/aisdb:latest --compress --file ./Dockerfile .. 
  docker push meridiancfi/aisdb:latest
fi

