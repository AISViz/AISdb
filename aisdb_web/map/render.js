/** @module render */
import html2canvas from 'html2canvas';

import { update_vesseltype_styles, waitForSearchState } from './selectform';

/** callback to capture the current map canvas as PNG base64image.
 * a download link is appended to the document, which will click itself,
 * then remove itself.
 */
async function screenshot_callback() {
  await html2canvas(document.querySelector('.map')).then((canvas) => {
    let dl = `${document.getElementById('time-select-start').value}-`;
    dl = `${dl}${document.getElementById('time-select-end').value}_`;
    dl = `${dl}${document.getElementById('vesseltype-select').value}`;
    dl = `${dl}.png`;
    let a = document.createElement('a');
    a.download = dl;
    const base64image = canvas.toDataURL('image/png');
    a.href = base64image;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
  });
}
window.screnshot_single = screenshot_callback;

/** take a screenshot using screenshot_callback() of each vessel type specified
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
 * @param {Object} opts render options
 */
async function screenshot(opts) {
  await waitForSearchState();

  if (opts === undefined) {
    opts = {
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
  }

  const vesseltypeselect = document.getElementById('vesseltype-select');
  for (let type of opts.renders) {
    vesseltypeselect.value = type;
    await new Promise((r) => {
      return setTimeout(r, 500);
    });
    update_vesseltype_styles();
    await new Promise((r) => {
      return setTimeout(r, 500);
    });
    await screenshot_callback();
  }
}

window.screenshot = screenshot;

export { screenshot };
