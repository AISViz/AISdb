import express from 'express';
import { currentdir, start_interface } from './server_module.js';

// App config
const app = express();
const port = 8080;

app.use('/', express.static(`${currentdir}/dist_map`));

start_interface(app, port);
