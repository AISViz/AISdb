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
COPY --chown="$USERNAME" docker_entry.py .
COPY --chown="$USERNAME" examples/ examples/
COPY --chown="$USERNAME" tests/ tests/
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN chown -R "$USERNAME" aisdb
ENTRYPOINT ["python", "./docker_entry.py"]
CMD ["/sbin/sshd", "-D", "-e", "-h", "/run/secrets/host_ssh_key", "-oAuthorizedKeysFile=/run/secrets/host_authorized_keys", "-oDenyUsers=root", "-oKbdInteractiveAuthentication=no", "-oPasswordAuthentication=no", "-oPermitEmptyPasswords=no", "-oPrintMotd=no", "-oPort=22", "-oPubkeyAuthentication=yes", "-oUseDNS=no", "-oX11Forwarding=yes", "-oX11UseLocalhost=no"]

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
COPY --chown="$USERNAME" js/ js/
CMD ["npm", "--prefix", "/home/ais_env/js/", "start"]

