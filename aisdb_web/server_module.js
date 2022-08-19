import { dirname } from 'path';
import { fileURLToPath } from 'url';
import { networkInterfaces } from 'os';


const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

function start_interface(app, port) {
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

        console.log(`server listening on http://${addr}:${port}`);

        break loop1;
      }
    }
  });
}

export {
  __dirname,
  start_interface,
};

