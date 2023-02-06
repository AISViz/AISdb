#!/bin/bash

set -e

cd $HOME

# software updates
sudo apt-get update -y
sudo apt-get upgrade -y

# install cargo
[[ ! -f $HOME/.cargo/bin/rustup ]] && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
source $HOME/.cargo/env
CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse cargo install mproxy-client


# install the receiver service
[[ ! -f $HOME/ais_rcv.service ]] && cat <<EOF > "$HOME/ais_rcv.service"
[Unit]
Description="AISDB Receiver"
After=network-online.target
Documentation=https://aisdb.meridian.cs.dal.ca/doc/receiver.html

[Service]
Type=simple
User=$USER
ExecStart=$HOME/.cargo/bin/mproxy-client --path /dev/ttyACM0 --server-addr 'aisdb.meridian.cs.dal.ca:9921'
Restart=always
RestartSec=30

[Install]
WantedBy=default.target
EOF

# start and enable the service at boot
sudo systemctl enable systemd-networkd-wait-online.service
sudo systemctl link ./ais_rcv.service
sudo systemctl daemon-reload
sudo systemctl enable ais_rcv
sudo systemctl start ais_rcv