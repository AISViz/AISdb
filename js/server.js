const { networkInterfaces } = require('os');

const express = require('express');
const app = express();
const port = 8085;


app.use(express.static('../docs/html'))

app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store')
    next()
})

app.listen(port, () => {
  console.log(`Documentation available at http://${networkInterfaces()["eth0"][0]["address"]}:${port}`);
})

