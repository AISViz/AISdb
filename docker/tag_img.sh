#!/bin/bash
echo "GIT CONTEXT:"
echo
find .. | grep -vi "`cat ../.gitignore`" | grep -vi '\/.git\/'
echo
echo
echo "CAUTION: TAGGING PUBLIC IMAGE WITH BUILD CONTEXT:"
echo
find .. | grep -vi "`cat ../.dockerignore`"

echo
read -p "Are you sure? [y/n]" -r
if [[ $REPLY =~ ^[Yy]$ ]]
then
  docker build --target webserv --tag meridiancfi/aisdb:latest --no-cache --compress --file ./Dockerfile .. 
  docker push meridiancfi/aisdb:latest
fi

