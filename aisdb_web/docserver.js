import { __dirname, start_interface } from './server_module.js';
import express from 'express';

// app config
const app = express();
const port = 8081;

app.use('/', express.static(`${__dirname }/dist_sphinx`));
app.use('/coverage', express.static(`${__dirname }/dist_coverage`));

start_interface(app, port);

