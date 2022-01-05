# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
   && pacman -S --noconfirm --needed \
      gcc 
      #python \
      #rust
      #qgis \
ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python3
WORKDIR "/home/$USERNAME"


# build and test rust targets
FROM aisdb as compile
#USER root
RUN pacman -S pkgconf rust --noconfirm
ARG USERNAME
USER "$USERNAME"
RUN mkdir -p aisdb aisdb_rust/src \
  && echo 'fn main(){}' > aisdb_rust/src/bin.rs \
  && echo 'fn main(){}' > aisdb_rust/src/lib.rs 
COPY --chown="$USERNAME" aisdb_rust/Cargo.toml aisdb_rust/Cargo.toml
RUN cargo build \
    --manifest-path=/home/"$USERNAME"/aisdb_rust/Cargo.toml \
    --release
COPY --chown="$USERNAME" aisdb_rust/ aisdb_rust/
RUN cargo build \
    --manifest-path=/home/"$USERNAME"/aisdb_rust/Cargo.toml \
    --release
RUN mkdir -p testdata/
CMD ["cargo", "test", "--manifest-path=aisdb_rust/Cargo.toml", "--release", "--color=always", "--", "--nocapture" ]

# sphinx docs + rust cargo docs
FROM compile AS webserv
USER root
RUN pacman -S --noconfirm --needed nodejs npm python python-sphinx python-sphinx_rtd_theme
ARG USERNAME
USER "$USERNAME"
RUN npm install express
COPY --chown="$USERNAME" docker/ docker/
COPY --chown="$USERNAME" readme.rst ./
COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" aisdb/ aisdb/ readme.rst ./
RUN /bin/bash /home/"$USERNAME"/docs/sphinxbuild.sh
CMD ["npm", "--prefix", "docker/", "start"]

# run package tests
FROM aisdb AS runtest
RUN pacman -S --noconfirm python qgis
ARG USERNAME
#RUN python -m pip install pytest --no-warn-script-location
USER "$USERNAME"
RUN python -m ensurepip && python -m pip install --no-warn-script-location --upgrade wheel pip numpy pytest
COPY --chown="$USERNAME" ./setup.py "/home/$USERNAME/setup.py"
RUN mkdir -p aisdb/database aisdb/webdata && python -m pip install . --no-warn-script-location
COPY --chown="$USERNAME" aisdb/ aisdb/
COPY --chown="$USERNAME" tests/ tests/
#COPY --chown="$USERNAME" aisdb/ tests/ .
CMD ["python", "-m", "pytest", "--color=yes", "-x", "--tb=native", "tests/"]

# forward X11 on sshd host
FROM runtest AS sshhost
USER root
RUN pacman -S --noconfirm --needed openssh xorg-xauth 
COPY --chown="$USERNAME" docker/entry.py examples/ .
#COPY --chown="$USERNAME" examples/ examples/
#COPY --chown="$USERNAME" aisdb/ aisdb/
ENTRYPOINT ["python", "./entry.py"]

