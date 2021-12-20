const { networkInterfaces } = require('os');

const express = require('express');
const app = express();
const port = 8085;


app.use(express.static('../docs/html'))

app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store')
    next()
})

app.listen(port, '::', () => {
  for (key of Object.keys(networkInterfaces())) {
    if (['lo', '127.0.0.1', '::1'].indexOf(key) >= 0 ) continue;
    for (net of networkInterfaces()[key]) {
      if (net['family'] == 'IPv4') addr = net['address'];
      else if (net['family'] == 'IPv6' && net['scopeid'] == 0) addr = `[${net['address']}]`;
      else continue;
      console.log(`Docs available at http://${addr}:${port}`);
    }
  }
})

