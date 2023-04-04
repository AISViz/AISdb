FROM node:19-alpine

RUN apk add binaryen build-base clang wasm-pack

WORKDIR /src
RUN npm install --save-dev vite

# compile wasm component dependencies
#COPY client_webassembly/Cargo.toml client_webassembly/Cargo.lock /src/client_webassembly/
#RUN mkdir -p /src/client_webassembly/src && echo 'fn main(){}' > /src/client_webassembly/src/lib.rs
#RUN cd /src/client_webassembly && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse wasm-pack build --release --target web --out-dir /src/aisdb_web/map/pkg

# install nodejs dependencies
#COPY aisdb_web/package.json aisdb_web/package-lock.json /src/aisdb_web/
#RUN npm install --prefix /src/aisdb_web

# build wasm components
COPY client_webassembly/ client_webassembly/
RUN cd client_webassembly && CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse wasm-pack build --release --target web --out-dir /src/aisdb_web/map/pkg
RUN wasm-opt -O3 -o /src/aisdb_web/map/pkg/client_bg.wasm /src/aisdb_web/map/pkg/client_bg.wasm 

# minify source at runtime via entrypoint
# this allows configuration of bundled JS code using environment variables defined at runtime
# see more about env args in aisdb_web/map/constants.js
RUN cat <<\EOF >entrypoint.sh
#!/bin/sh
echo "Packaging AISDB JavaScript assets..."
npx vite build /src/aisdb_web/map --outDir /src/aisdb_web/dist_map
exec "$@"
EOF
RUN chmod +x /src/entrypoint.sh

# bundle and minify website content
COPY aisdb_web/ /src/aisdb_web/
RUN npm install --prefix /src/aisdb_web --include=dev

ENTRYPOINT ["/bin/sh", "/src/entrypoint.sh"]

CMD ["npm", "--prefix", "/src/aisdb_web", "start"]
