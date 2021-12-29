# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
   && pacman -S --noconfirm --needed \
      gcc \
      python \
      qgis \
      rust
ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python3
USER "$USERNAME"
WORKDIR "/home/$USERNAME"
COPY --chown="$USERNAME" setup.py .
RUN python -m ensurepip && python -m pip install --no-warn-script-location --upgrade wheel pip numpy 
RUN mkdir -p aisdb/database aisdb/webdata && python -m pip install . --no-warn-script-location

# build and test rust targets
FROM aisdb as compile
ARG USERNAME
#WORKDIR "/home/$USERNAME/aisdb/rust"
COPY --chown="$USERNAME" rust/ rust/
RUN cargo build --manifest-path="/home/$USERNAME/rust/Cargo.toml" --release
CMD ["cargo", "test", "--manifest-path=rust/Cargo.toml"]

# run package tests
FROM aisdb AS runtest
ARG USERNAME
USER "$USERNAME"
RUN python -m pip install pytest --no-warn-script-location
COPY --chown="$USERNAME" aisdb/ aisdb/
COPY --chown="$USERNAME" tests/ tests/
CMD ["python", "-m", "pytest", "--color=yes", "-x", "--tb=native", "tests/"]

# forward X11 on sshd host
FROM aisdb AS sshhost
USER root
RUN pacman -S --noconfirm --needed openssh xorg-xauth
COPY --chown="$USERNAME" docker/entry.py .
COPY --chown="$USERNAME" examples/ examples/
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN chown -R "$USERNAME" aisdb
ENTRYPOINT ["python", "./entry.py"]

# sphinx docs
FROM aisdb AS webserv
USER root
RUN pacman -S --noconfirm --needed nodejs npm python-sphinx python-sphinx_rtd_theme
ARG USERNAME
USER "$USERNAME"
RUN npm install express
COPY --chown="$USERNAME" readme.rst .
COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN /bin/bash "/home/$USERNAME/docs/sphinxbuild.sh"
COPY --chown="$USERNAME" docker/ docker/
CMD ["npm", "--prefix", "docker/", "start"]

