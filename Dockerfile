FROM ubuntu:latest
LABEL authors="ruixin"

ENTRYPOINT ["top", "-b"]