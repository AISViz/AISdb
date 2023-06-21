import {
  db_socket_host , 
  vesselInfo,
} from './clientsocket.js';
import { lineSource } from './map.js';
import { set_track_style } from './selectform.js';

let db_ready = false;
/**await until socket has returned metadata */
async function waitForDB() {
  while (db_ready === false) {
    await new Promise((resolve) => {
      return setTimeout(resolve, 50);
    });
  }
}

interface DictInterface{
    [index: string]: string;
} 

const meta_keys_display = {
  mmsi: 'MMSI',
  imo: 'IMO',
  vessel_name: 'Name',
  vessel_name2: 'Name',
  flag: 'Flag',
  ship_type_txt: 'Type',
  vesseltype_generic: 'Type',
  vesseltype_detailed: 'Details',
  length_breadth: 'Size',
  year_built: 'Year',
  summer_dwt: 'Summer DWT',
  gross_tonnage: 'Gross Tonnage',
} as DictInterface;

interface ResponseInterface { [key: string]: any }

function handle_vesselinfo(response: ResponseInterface) {
  //console.log(debug_count);
  //debug_count = debug_count + 1;
  //let meta_string ;
  let meta_string: string;

  for (const key in meta_keys_display) {
    //Const value = meta_keys_display[key];
    const value = response[key];

    //}
    //for (const [ key, value ] of Object.entries(response)) {
    if (value === '' || key.includes('dim_') || key === 'ship_type') {
      continue;
    }

    if (key === 'vessel_name' && 'vessel_name2' in response) {
      continue;
    }

    if (key === 'ship_type_txt' && 'vesseltype_generic' in response && response.vesseltype_generic !== '-') {
      continue;
    }

    if ((value === '0') && key !== 'mmsi') {
      continue;
    }

    if (key === 'msgtype' || value === undefined || value === '-') {
      continue;
    }

    //Meta_string = `${meta_string}${meta_keys_display[key]}: ${value}&emsp;`;

    meta_string = 
      `${meta_keys_display[key]}: ${value}<br>`;
  }
  //response.meta_string = meta_string;
  const idx: string = response.mmsi;
  vesselInfo[idx] = response;
  const ft = lineSource.getFeatureById(response.mmsi);
  if (ft !== null) {
    set_track_style(ft);
  }
  return response;
}

const meta_socket: WebSocket = new WebSocket(db_socket_host);

meta_socket.addEventListener('message', async (socketEvent) => {
  const txt = await socketEvent.data.text();
  const response = JSON.parse(txt);

  switch (response.msgtype) {
    case 'vesselinfo': {
      const metadata = handle_vesselinfo(response);
      vesselInfo[metadata.mmsi] = metadata;
      //meta_socket.send('ack');
      break;
    }

    case 'doneMetadata': {
      db_ready = true;
      //meta_socket.send('ack');
      meta_socket.close();
      //console.log('done metadata initialization');
      break;
    }
  }
});

meta_socket.onerror = () => {
  meta_socket.close();
};
meta_socket.onopen = async (_event: Event) => {
  //console.log('sending request...');
  //meta_socket.send(JSON.stringify({ msgtype: 'meta' }));
  await meta_socket.send(JSON.stringify({ msgtype: 'meta' }));
  await waitForDB();
  meta_socket.close();
};




