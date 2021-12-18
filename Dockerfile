FROM archlinux:base AS aisdb
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

COPY --chown="$USERNAME" setup.py .

RUN mkdir -p aisdb/database aisdb/webdata \
  && python -m pip install . --no-warn-script-location

#COPY --chown="$USERNAME" docs/ docs/
COPY --chown="$USERNAME" examples/ examples/
COPY --chown="$USERNAME" tests/ tests/
COPY --chown="$USERNAME" aisdb/ aisdb/

USER root


FROM aisdb AS webserv

RUN pacman -S nodejs npm python-sphinx --noconfirm
ARG USERNAME
USER "$USERNAME"
RUN npm install express

COPY --chown="$USERNAME" readme.rst .
COPY --chown="$USERNAME" docs/ docs/
RUN /bin/bash "/home/$USERNAME/docs/sphinxbuild.sh"
COPY --chown="$USERNAME" js/ js/

