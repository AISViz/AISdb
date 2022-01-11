# base dependencies
FROM archlinux:base AS aisdb
# https://wiki.archlinux.org/title/Pacman/Tips_and_tricks#Removing_everything_but_essential_packages
RUN pacman -Syyuu --noconfirm \
  && pacman -S python rust --noconfirm \
  && pacman -D --asdeps `pacman -Qqe` \
  && pacman -D --asexplicit --noconfirm python rust pacman systemd \
  && pacman -Rncs --noconfirm `pacman -Qtdq` 
ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python
WORKDIR "/home/$USERNAME"


# run package tests
FROM aisdb AS runtest
RUN pacman -S --noconfirm --needed qgis nodejs npm python-sphinx python-sphinx_rtd_theme
ARG USERNAME
#RUN mkdir -p ais && chown "$USERNAME" ais
USER "$USERNAME"
RUN python -m ensurepip && python -m pip install --no-warn-script-location --upgrade wheel pip numpy pytest
COPY --chown="$USERNAME" ./setup.py "/home/$USERNAME/setup.py"
RUN mkdir -p ais ais/tmp_parsing ais/zones ais/rawdata_dir ais/scriptoutput aisdb/database aisdb/webdata \
  && python -m pip install . --no-warn-script-location 
COPY --chown="$USERNAME" tests/ tests/
COPY --chown="$USERNAME" aisdb/ aisdb/ 
# disable tests that require X11 or web data sources
CMD ["python", "-m", "pytest", "--color=yes", "--tb=native", "-s", "tests/", "-k", "not qgis and not merge" ]


# forward X11 on sshd host
FROM runtest AS sshhost
USER root
RUN pacman -S --noconfirm --needed openssh xorg-xauth 
COPY --chown="$USERNAME" docker/entry.py ./entry.py
ENTRYPOINT ["python", "./entry.py"]


# build and test rust targets
FROM aisdb as compile
ARG USERNAME
USER "$USERNAME"
RUN mkdir -p aisdb_rust/src \
  && echo 'fn main(){}' > aisdb_rust/src/db.rs \
  && echo 'fn main(){}' > aisdb_rust/src/decode.rs \
  && echo 'fn main(){}' > aisdb_rust/src/util.rs \
  && echo 'fn main(){}' > aisdb_rust/src/bin.rs
COPY --chown="$USERNAME" aisdb_rust/Cargo.toml aisdb_rust/Cargo.toml
RUN cargo build \
    --manifest-path=/home/"$USERNAME"/aisdb_rust/Cargo.toml \
    --release
COPY --chown="$USERNAME" aisdb_rust/ aisdb_rust/
RUN cargo build \
    --manifest-path=/home/"$USERNAME"/aisdb_rust/Cargo.toml \
    --release
RUN mkdir -p testdata/ && chmod -R "$USERNAME" .
CMD ["cargo", "test", "--manifest-path=aisdb_rust/Cargo.toml", "--release", "--color=always", "--", "--nocapture" ]


# sphinx docs + rust cargo docs
FROM runtest AS webserv
ARG USERNAME
USER "$USERNAME"
RUN npm install express
COPY --chown="$USERNAME" nodejs/ nodejs/
COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" readme.rst ./readme.rst
COPY --chown="$USERNAME" aisdb_rust/ aisdb_rust/
RUN /bin/bash /home/"$USERNAME"/docs/sphinxbuild.sh
CMD ["npm", "--prefix", "nodejs/", "start"]
