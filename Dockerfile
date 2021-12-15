FROM archlinux:base
RUN pacman -Syyuu --noconfirm \
   && pacman -S --noconfirm --needed \
      gcc \
      openssh \
      python \
      qgis \
      xorg-xauth

# configure a non-root user to run the application, disable password authentication
ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python3 
USER "$USERNAME"

WORKDIR "/home/$USERNAME"

RUN python -m ensurepip
RUN python -m pip install --upgrade wheel pip && python -m pip install --upgrade numpy
COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" setup.py .
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN python -m pip install . 

USER root
CMD ["/sbin/sshd", \
      "-D", \
      "-e", \
      "-h", "/run/secrets/host_ssh_key", \
      "-oAuthorizedKeysFile=/run/secrets/host_authorized_keys", \
      "-oDenyUsers=root", \
      "-oKbdInteractiveAuthentication=no", \
      "-oPasswordAuthentication=no", \
      "-oPermitEmptyPasswords=no", \
      "-oPrintMotd=no", \
      "-oPort=22", \
      "-oPubkeyAuthentication=yes", \
      "-oUseDNS=no", \
      "-oX11Forwarding=yes", \
      "-oX11UseLocalhost=no" ] 


