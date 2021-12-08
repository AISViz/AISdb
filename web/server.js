const http = require('http');
const express = require('express');

const requestListener = function(req, res) {
  res.writeHead(200);
  res.end('hello world');
}

const server = http.createServer(requestListener);
server.listen(8084);
