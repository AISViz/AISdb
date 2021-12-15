FROM archlinux:base
RUN pacman -Syyuu --noconfirm \
   && pacman -S --noconfirm --needed \
      gcc \
      openssh \
      python \
      qgis \
      xorg-xauth

ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python3 
USER "$USERNAME"

WORKDIR "/home/$USERNAME"

RUN python -m ensurepip \
  && python -m pip install --no-warn-script-location --upgrade wheel pip numpy 

COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" setup.py .

RUN mkdir -p aisdb/database aisdb/webdata \
  && python -m pip install . --no-warn-script-location

COPY --chown="$USERNAME" aisdb/ aisdb/

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


