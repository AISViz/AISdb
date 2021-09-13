## Setting up your own AIS radio station

#### Requirements
  - Any device running linux with a constant power supply and internet connection, for example, a Raspberry Pi
    - The easiest way to supply power to the receiver/filter is via USB, so consider a device with USB ports for simplicity, although this is not necessary
  - 162MHz receiver, such as [Wegmatt dAISy 2 Channel Receiver](https://shop.wegmatt.com/collections/frontpage/products/daisy-2-dual-channel-ais-receiver-with-nmea-0183?variant=7103563628580)
  - An antenna in the VHF frequency band (30MHz - 300MHz). If your antenna doesn't have a BNC type connector, don't forget an adapter. Example: [Shakespeare QC-4 VHF Antenna](https://shakespeare-ce.com/marine/product/qc-4-quickconnect-vhf-antenna/)
  - Optionally, you may also want
    - Antenna mount
    - A filtered preamp, such as [this one sold by Uputronics](https://store.uputronics.com/index.php?route=product/product&path=59&product_id=93), to improve signal range


#### Setting up the hardware
  - When setting up your antenna, be sure to place it as high as possible and as far away from obstructions and other equipment as is practical.
  - Connect the antenna to the receiver. If using a preamp filter, this should be connected in between the antenna and the receiver.
  - Connect the receiver to your linux device via USB cable. If using a preamp filter, power this with a USB cable also
  - Validate the hardware configuration
    - When connected via USB, The AIS receiver is usually located under /dev/ with a name beginning with ttyACM, for example /dev/ttyACM0. Ensure that a device is present here
    - To test that the receiver is receiving messages, use the command `sudo cat /dev/ttyACM0` to print output received by the device. If all is working as intended, you should see some streams of bytes appearing on the screen.


#### Setting up the software
  - Install AISHub dispatcher on your device, following the [installation instructions on the AISHub website](https://www.aishub.net/ais-dispatcher#linux)
  - Configure your dispatcher to send data to any endpoints as desired, such as [AISHub](https://www.aishub.net/join-us), [VesselFinder](https://stations.vesselfinder.com/become-partner), or [MarineTraffic](https://www.marinetraffic.com/en/p/expand-coverage). Many of these sites will agree to share other AIS data with you in exchange for sending your datastream with them


#### Troubleshooting
  - Permission Denied Error from the dispatcher when trying to access the receiver over serial (USB) connection: If this occurs, ensure the 'ais' user is added to any groups that may be necessary to read data from serial USB devies, such as uucp or dialout. If this still fails after a reboot, you can try giving ownership of the serial device to the AIS user. 
