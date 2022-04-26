const { networkInterfaces } = require('os');
const express = require('express');

// app config
const app = express();
const port = 8080;

/*
  // CSRF middleware
let csrf = require('csurf');
let cookieParser = require('cookie-parser');
let bodyParser = require('body-parser');

// middleware routing
 let csrfProtection = csrf({ cookie: { sameSite: 'strict' } });
 let parseForm = bodyParser.urlencoded({ extended: false });


// handle CSRF tokens
app.use(cookieParser());
app.get('/mapselect', csrfProtection, (req, res) => {
  res.render('send', { csrfToken: req.csrfToken() });
});
*/

// start the application
// app.use('/', express.static('../docs/html'));
app.use('/', express.static('./dist'));
// app.use('/assets', express.static('../aisdb_web/dist/assets'));
// app.use('/manifest.json', express.static('./manifest.json'));
// app.use('/favicon.svg', express.static('./favicon.svg'));
// app.use('/robots.txt', express.static('./robots.txt'));


app.listen(port, '::', () => {
  loop1: for (let key of Object.keys(networkInterfaces()).reverse()) {
    if ([ 'lo', '127.0.0.1', '::1' ].indexOf(key) >= 0) {
      continue loop1;
    }

    let addr = null;
    loop2: for (let net of networkInterfaces()[key].reverse()) {
      if (net.family === 'IPv4') {
        addr = net.address;
      } else if (net.family === 'IPv6' && net.scopeid === 0) {
        addr = `[${net.address}]`;
      } else {
        continue loop2;
      }

      console.log(`Docs available at http://${addr}:${port}\n`);

      break loop1;
    }
  }
});

