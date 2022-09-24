import { currentdir, start_interface } from './server_module.js';
import express from 'express';

// app config
const app = express();
const port = 8081;

app.use('/', express.static(`${currentdir}/dist_sphinx`));
app.use('/coverage', express.static(`${currentdir}/dist_coverage`));

start_interface(app, port);

