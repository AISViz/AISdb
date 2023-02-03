Setting up an AIS base station
==============================

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

 #. Install Raspberry Pi OS with SSH enabled https://www.raspberrypi.com/software/ . If using the RPi imager, be sure to run it as an administrator.
 #. Connect the receiver to the raspberry pi using the USB cable. Login to the pi and update the system ``sudo apt-get update``
 #. Install the rust toolchain on the raspberry pi ``curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh``
 #. Log out and log back in to add rust and cargo to the system path
 #. Install the network client and dispatcher from crates.io ``cargo install mproxy-client mproxy-forward``
     * To install from source, use the local path instead e.g. ``cargo install --path ./dispatcher/client``
 #. Install new systemd services to run the AIS receiver and dispatcher, replacing ``User=ais`` and ``/home/ais`` with the name selected in step 1.


Create a new text file ``./ais_rcv.service`` with contents:

.. code-block:: 
    :caption: ./ais_rcv.service

    [Unit]
    Description="AISDB Receiver"
    After=network-online.target

    [Service]
    Type=simple
    User=ais
    ExecStart=/home/ais/.cargo/bin/mproxy-client --path /dev/ttyACM0 --server-addr '[::1]:9901'
    Restart=always
    RestartSec=30

    [Install]
    WantedBy=default.target



Create a new text file ``./ais_upstream.service`` with contents:

.. code-block:: 
    :caption: ./ais_upstream.service

    [Unit]
    Description="AISDB Dispatcher"
    After=network-online.target

    [Service]
    Type=simple
    User=ais
    ExecStart=/home/ais/.cargo/bin/mproxy-forward --udp-listen-addr '[::]:9901' --tcp-connect-addr 'aisdb.meridian.cs.dal.ca:9920'
    Restart=always
    RestartSec=30

    [Install]
    WantedBy=default.target


These services will broadcast receiver input on local UDP port 9901, and then forward it upstream to aisdb.meridian.cs.dal.ca via TCP. 
Additional local or remote endpoints can be added at this step, for more info see ``mproxy-forward --help``, as well as additional AIS networking tools ``mproxy-server`` and ``mproxy-reverse`` located in the ``./dispatcher`` source directory.
The UDP Multicast channel may be used to proxy multiple interfaces simultaneously.

Next, link and enable the services on the rpi. This will allow the receiver to be started at boot

.. code-block:: bash

    sudo systemctl link ./ais_rcv.service
    sudo systemctl link ./ais_upstream.service
    sudo systemctl daemon-reload
    sudo systemctl enable systemd-networkd-wait-online.service

    sudo systemctl enable ais_rcv
    sudo systemctl start ais_rcv

    sudo systemctl enable ais_upstream
    sudo systemctl start ais_upstream


See more examples in ``docker-compose.yml``

