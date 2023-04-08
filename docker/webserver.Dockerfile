#FROM node:19-alpine
#RUN apk add binaryen build-base clang wasm-pack
#FROM node:latest
#RUN apt-get update -y \
#  && apt-get install -y build-essential binaryen clang
#RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
#RUN echo 'echo . $HOME/.cargo/env' >> /etc/profile
#RUN . $HOME/.cargo/env && curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh -s -- -y
#ENV PATH="$PATH:/root/.cargo/bin"



FROM rust:slim AS build_client_webassembly
RUN apt-get update -y
RUN apt-get install -y binaryen clang curl libssl-dev pkg-config
RUN curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh -s -- -y
ENV PATH="$PATH:/root/.cache/.wasm-pack/.wasm-bindgen-cargo-install-0.2.84/bin"

WORKDIR /src

# compile wasm component dependencies
#COPY client_webassembly/Cargo.toml client_webassembly/Cargo.lock /src/client_webassembly/
#RUN mkdir -p /src/client_webassembly/src && echo 'fn main(){}' > /src/client_webassembly/src/lib.rs
#RUN cd /src/client_webassembly && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse wasm-pack build --release --target web --out-dir /src/aisdb_web/map/pkg


# build wasm components
COPY client_webassembly/ client_webassembly/
RUN cd client_webassembly && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse wasm-pack build --release --target web --out-dir /src/aisdb_web/map/pkg
RUN wasm-opt -O3 -o /src/aisdb_web/map/pkg/client_bg.wasm /src/aisdb_web/map/pkg/client_bg.wasm 



FROM node:slim AS webserver
WORKDIR /src
RUN npm install --save-dev vite

# minify source at runtime via entrypoint
# this allows configuration of bundled JS code using environment variables defined at runtime
# see more about env args in aisdb_web/map/constants.js
RUN echo "#!/bin/sh\necho \"Packaging AISDB JavaScript assets...\"\nnpx vite build /src/aisdb_web/map --outDir /src/aisdb_web/dist_map\necho 'network hostname: `uname -n`'\nexec \"\$@\"" > /src/entrypoint.sh
RUN chmod +x /src/entrypoint.sh


# bundle and minify website content
COPY aisdb_web/ /src/aisdb_web/
COPY --from=build_client_webassembly /src/aisdb_web/map/pkg /src/aisdb_web/map/pkg
RUN npm install --prefix /src/aisdb_web --include=dev

ENTRYPOINT ["/bin/sh", "/src/entrypoint.sh"]

CMD ["npm", "--prefix", "/src/aisdb_web", "start"]
