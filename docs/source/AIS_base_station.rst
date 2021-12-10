Setting up a radio station
==========================

Requirements
------------

-  Any device running linux with a constant power supply and internet
   connection, for example, a Raspberry Pi

   -  The easiest way to supply power to the receiver/filter and read
      data from the receiver is via USB, so consider a device with USB
      ports for simplicity, although this is not necessary

-  162MHz receiver, such as `Wegmatt dAISy 2 Channel
   Receiver <https://shop.wegmatt.com/collections/frontpage/products/daisy-2-dual-channel-ais-receiver-with-nmea-0183?variant=7103563628580>`__
-  An antenna in the VHF frequency band (30MHz - 300MHz). If your
   antenna doesn’t have a BNC type connector, don’t forget an adapter.
   Example: `Shakespeare QC-4 VHF
   Antenna <https://shakespeare-ce.com/marine/product/qc-4-quickconnect-vhf-antenna/>`__
-  Optionally, you may also want

   -  Antenna mount
   -  A filtered preamp, such as `this one sold by
      Uputronics <https://store.uputronics.com/index.php?route=product/product&path=59&product_id=93>`__,
      to improve signal range and quality

Hardware Setup
--------------

-  When setting up your antenna, be sure to place it as high as possible
   and as far away from obstructions and other equipment as is
   practical.
-  Connect the antenna to the receiver. If using a preamp filter, this
   should be connected in between the antenna and the receiver.
-  Connect the receiver to your linux device via USB cable. If using a
   preamp filter, power this with a USB cable also
-  Validate the hardware configuration

   -  When connected via USB, The AIS receiver is usually located under
      /dev/ with a name beginning with ttyACM, for example /dev/ttyACM0.
      Ensure that a device is present here
   -  To test the receiver, use ``sudo cat /dev/ttyACM0`` to display its output.
      If all is working as intended, you should see some streams of
      bytes appearing on the screen.

   ::

      $ sudo cat /dev/ttyACM0
      !AIVDM,1,1,,A,B4eIh>@0<voAFw6HKAi7swf1lH@s,0*61
      !AIVDM,1,1,,A,14eH4HwvP0sLsMFISQQ@09Vr2<0f,0*7B
      !AIVDM,1,1,,A,14eGGT0301sM630IS2hUUavt2HAI,0*4A
      !AIVDM,1,1,,B,14eGdb0001sM5sjIS3C5:qpt0L0G,0*0C
      !AIVDM,1,1,,A,14eI3ihP14sM1PHIS0a<d?vt2L0R,0*4D
      !AIVDM,1,1,,B,14eI@F@000sLtgjISe<W9S4p0D0f,0*24
      !AIVDM,1,1,,B,B4eHt=@0:voCah6HRP1;?wg5oP06,0*7B
      !AIVDM,1,1,,A,B4eHWD009>oAeDVHIfm87wh7kP06,0*20

Software Setup
--------------

-  Install AISHub dispatcher on your device, following the `installation
   instructions on the AISHub
   website <https://www.aishub.net/ais-dispatcher#linux>`__
-  Configure your dispatcher to send data to any endpoints as desired,
   such as `AISHub <https://www.aishub.net/join-us>`__,
   `VesselFinder <https://stations.vesselfinder.com/become-partner>`__,
   or
   `MarineTraffic <https://www.marinetraffic.com/en/p/expand-coverage>`__.
   Many of these sites will agree to share other AIS data with you in
   exchange for sending your datastream to them
