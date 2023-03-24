FROM node:19-alpine

RUN apk add binaryen build-base clang wasm-pack

# compile wasm component dependencies
#COPY client_webassembly/Cargo.toml client_webassembly/Cargo.lock client_webassembly/
#RUN mkdir -p client_webassembly/src && echo 'fn main(){}' > client_webassembly/src/lib.rs
#RUN cd client_webassembly && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse wasm-pack build --release --target web --out-dir /aisdb_web/map/pkg

# install nodejs dependencies
#COPY aisdb_web/package.json aisdb_web/package-lock.json aisdb_web/
#RUN npm install --prefix /aisdb_web

# build wasm components
COPY client_webassembly/ client_webassembly/
RUN cd client_webassembly && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse wasm-pack build --release --target web --out-dir /aisdb_web/map/pkg
RUN wasm-opt -O3 -o /aisdb_web/map/pkg/client_bg.wasm /aisdb_web/map/pkg/client_bg.wasm 

# see more about env args in aisdb_web/map/constants.js
ARG VITE_AISDBHOST
ARG VITE_AISDBPORT
ARG VITE_BINGMAPTILES
ARG VITE_TILESERVER
ARG VITE_DISABLE_SSL_DB
ARG VITE_DISABLE_SSL_STREAM
ARG VITE_BINGMAPTILES
ARG VITE_NO_DB_LIMIT

# bundle and minify website content
COPY aisdb_web/ aisdb_web/
RUN npm install --prefix /aisdb_web
RUN cd aisdb_web && npx vite build map --outDir /aisdb_web/dist_map #--assetsInlineLimit 8192

CMD ["npm", "--prefix", "/aisdb_web", "start"]
