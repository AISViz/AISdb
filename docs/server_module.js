import { dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import { networkInterfaces } from 'node:os';

const currentdir = dirname(fileURLToPath(import.meta.url));

function start_interface(app, port) {
  app.listen(port, '::', () => {
    loop1: for (const key of Object.keys(networkInterfaces()).reverse()) {
      if ([ 'lo', '127.0.0.1', '::1' ].includes(key)) {
        continue;
      }

      let addr = null;
      for (const net of networkInterfaces()[key].reverse()) {
        if (net.family === 'IPv4') {
          addr = net.address;
        } else if (net.family === 'IPv6' && net.scopeid === 0) {
          addr = `[${net.address}]`;
        } else {
          continue;
        }

        console.log(`server listening on http://${addr}:${port}`);

        break loop1;
      }
    }
  });
}

export {
  currentdir,
  start_interface,
};
