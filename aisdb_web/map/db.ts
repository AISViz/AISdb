import {
  db_socket_host , 
  vesselInfo,
} from './clientsocket.js';
import { lineSource } from './map.js';
import { set_track_style } from './selectform.js';


const vesselInfoDB = window.indexedDB.open('AISDB', 15);

vesselInfoDB.onerror = (_event: Event) => {
  console.error('unable to connect VesselInfoDB!');
};

vesselInfoDB.onsuccess = (event: Event) => {
  //const vdb = vesselInfoDB.result;
  console.log(event.target);
  const tx = event.target.result.transaction('VesselInfoDB', 'readonly');
  const s = tx.objectStore('VesselInfoDB');

  s.openCursor().onsuccess = (cursor_event: Event) => {
    const cursor = cursor_event.target.result;
    if (cursor) {
      console.log(cursor.value);
      vesselInfo[cursor.key] = cursor.value;
      cursor.continue();
    } else {
      console.log('done loading metadata from indexedDB');
    }
  };

  db_ready = true;
};

/**
  initialize metadata local browser storage */

vesselInfoDB.onupgradeneeded = async (event: Event) => {

  const db = event.target.result;
  console.log('fetching db upgrade...');
  if (db.objectStoreNames.contains('VesselInfoDB')) {
    db.deleteObjectStore('VesselInfoDB');
  }
  const objStore = db.createObjectStore('VesselInfoDB', { keyPath: 'mmsi' });

  objStore.transaction.oncomplete = async (eventComplete: Event) => {

    /*  ------------ */
    console.log('trace');
    await waitForDB();
    console.log('trace2', vesselInfo);
    /*
       while (db_ready === false) {
       setTimeout(function(){}, 50);
       }
       */
    //await waitForTimerange();

    //vesselInfoTX = vesselObjStore;
    //await waitForSocket();
    //await db_socket.send(JSON.stringify({ msgtype: 'meta' }));
    //await waitForMetadata();
    const vesselObjStore = db.transaction('VesselInfoDB', 'readwrite').objectStore('VesselInfoDB');
    //const vesselObjStore = eventComplete.target.db.transaction('VesselInfoDB', 'readwrite').objectStore('VesselInfoDB');
    let i = 0;
    for (const [ key, val ] of Object.entries(vesselInfo)) {
      i = i + 1;
      if (key === undefined || key === null || key === 'undefined') {
        console.error('failed to add ', val);
        continue;
      }
      console.log('adding ', val);
      vesselObjStore.add(val);
    }
    console.log('finished storing vessel metadata');
  };
};

export {
   // vesselInfoDB,
};
