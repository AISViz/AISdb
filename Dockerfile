#FROM python:3
#FROM python:3.11-rc-alpine
FROM archlinux:base
RUN   pacman -Syyuu --noconfirm \
   && pacman -S --noconfirm --needed \
      gcc \
      openssh \
      python \
      qgis \
      wget \
      xorg-xauth

      #git \
      #base-devel \

# configure a non-root user to run the application, disable password authentication
#USER root
ARG USERNAME
RUN useradd -m "$USERNAME" --shell /bin/python3 
  # && echo "$USERNAME ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers
USER "$USERNAME"

WORKDIR "/home/$USERNAME"

RUN python -m ensurepip
RUN python -m pip install --upgrade wheel pip && python -m pip install --upgrade numpy
COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" setup.py .
COPY --chown="$USERNAME" aisdb/ aisdb/
RUN python -m pip install . 

#CMD ["python", "./docker_entry.py"]
#CMD ["python", "-B","-I","-i","-c", "print(\"hello world\")"]
#RUN [[ ! -f "/etc/ssh/host_key" ]] && ssh-keygen -t ed25519 -f "/etc/ssh/host_key" 

#CMD ["/sbin/sshd", "-D", "-e", "-f", "/run/secrets/host_ssh_conf", "-h", "/run/secrets/host_ssh_key"] 
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



