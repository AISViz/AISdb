const express = require('express');
const app = express();
const port = 8085;


app.use(express.static('../docs/html'))

app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store')
    next()
})


app.listen(port, () => {
  console.log(`listening on localhost:${port}`)
})

