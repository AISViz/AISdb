/** @module render */
import html2canvas from 'html2canvas';

import { update_vesseltype_styles, waitForSearchState } from './selectform.js';
import { lineSource } from './map.js';

/** Callback to capture the current map canvas as PNG base64image.
 * a download link is appended to the document, which will click itself,
 * then remove itself.
 */
async function screenshot_callback() {
  await html2canvas(document.querySelector('.map')).then((canvas) => {
    let dl = `${document.querySelector('#time-select-start').value}-`;
    dl = `${dl}${document.querySelector('#time-select-end').value}_`;
    dl = `${dl}${document.querySelector('#vesseltype-select').value}`;
    dl = `${dl}.png`;
    const a = document.createElement('a');
    a.download = dl;
    const base64image = canvas.toDataURL('image/png');
    a.href = base64image;
    document.body.append(a);
    a.click();
    a.remove();
  });
}

window.screnshot_single = screenshot_callback;

/** Take a screenshot using screenshot_callback() of each vessel type specified
 * in opts. if opts is undefined, the following vesseltypes will be used:
 * opts = {
      renders: [
        'All',
        'Cargo',
        'Tanker',
        'Fishing',
        'Tug',
        'Pleasure Craft',
        'Passenger',
        'None',
      ]
    };
 * @param {Object} options render options
 */
async function screenshot(options) {
  await waitForSearchState();

  if (options === undefined) {
    options = {
      renders: [
        'All',
        'Cargo',
        'Tanker',
        'Fishing',
        'Tug',
        'Pleasure Craft',
        'Passenger',
        'None',
      ],
    };
  }

  const vesseltypeselect = document.querySelector('#vesseltype-select');
  for (const type of options.renders) {
    vesseltypeselect.value = type;
    await new Promise((r) => {
      return setTimeout(r, 500);
    });
    update_vesseltype_styles(lineSource);
    await new Promise((r) => {
      return setTimeout(r, 500);
    });
    await screenshot_callback();
  }
}

window.screenshot = screenshot;

export { screenshot };
