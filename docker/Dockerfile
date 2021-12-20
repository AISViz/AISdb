# base dependencies
FROM archlinux:base AS aisdb
RUN pacman -Syyuu --noconfirm \
   && pacman -S --noconfirm --needed \
      gcc \
      python \
      qgis
ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python3
USER "$USERNAME"
WORKDIR "/home/$USERNAME"
COPY --chown="$USERNAME" setup.py .
RUN python -m ensurepip && python -m pip install --no-warn-script-location --upgrade wheel pip numpy 
RUN mkdir -p aisdb/database aisdb/webdata && python -m pip install . --no-warn-script-location

# forward X11 on sshd host
FROM aisdb AS sshhost
USER root
RUN pacman -S --noconfirm --needed openssh xorg-xauth
COPY --chown="$USERNAME" docker/entry.py .
COPY --chown="$USERNAME" examples/ examples/
COPY --chown="$USERNAME" tests/ tests/
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN chown -R "$USERNAME" aisdb
ENTRYPOINT ["python", "./entry.py"]

# run package tests
FROM aisdb AS runtest
ARG USERNAME
USER "$USERNAME"
RUN python -m pip install pytest --no-warn-script-location
COPY --chown="$USERNAME" tests/ tests/
COPY --chown="$USERNAME" aisdb/ aisdb/
CMD ["python", "-m", "pytest", "--color=yes", "-x", "--tb=native", "tests/"]

# sphinx docs
FROM aisdb AS webserv
USER root
RUN pacman -S --noconfirm --needed nodejs npm python-sphinx 
ARG USERNAME
USER "$USERNAME"
RUN npm install express
COPY --chown="$USERNAME" readme.rst .
COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN /bin/bash "/home/$USERNAME/docs/sphinxbuild.sh"
COPY --chown="$USERNAME" docker/ docker/
CMD ["npm", "--prefix", "/home/ais_env/docker/", "start"]

