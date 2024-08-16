#!/bin/bash
SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
ROOTDIR="${SCRIPTPATH}/.."
PKGDIR="${ROOTDIR}/aisdb"
RSTSOURCEDIR="${ROOTDIR}/docs/source"
MAPDIR="${ROOTDIR}/aisdb_web/map"
SPHINXDIR="${ROOTDIR}/docs/dist_sphinx"
WASMDIR="${ROOTDIR}/client_webassembly"

set -e

# jsdocs site build
#JSDOCDIR="${ROOTDIR}/aisdb_web/dist_jsdoc"
#cd "${SCRIPTPATH}"
#npx jsdoc \
#  --recurse "${MAPDIR}" \
#  --package "${ROOTDIR}/aisdb_web/package.json" \
#  --destination "${JSDOCDIR}"



# webassembly components build for map
#[[ ! -f "$HOME/.cargo/bin/wasm-pack" ]] && echo "installing wasm-pack..." && curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
cd "${WASMDIR}"
if declare -p DEBUG ; then
  echo 'building with profile dev ...'
  wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --dev
else 
  echo 'building with profile release ...'
  wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --release
fi
wasm-opt -O3 -o "${MAPDIR}/pkg/client_bg.wasm" "${MAPDIR}/pkg/client_bg.wasm"
#wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --dev


# build map webapp
cd "${MAPDIR}" 
npm --prefix "${ROOTDIR}/aisdb_web" install
#VITE_AISDBHOST=$AISDBHOST \
#VITE_AISDBPORT=$AISDBPORT \
#VITE_BINGMAPTILES=$VITE_BINGMAPTILES \
#VITE_TILESERVER=$VITE_TILESERVER \

#VITE_BINGMAPTILES=\
#VITE_TILESERVER=\
VITE_DISABLE_SSL_DB=1 \
VITE_DISABLE_STREAM=1 \
VITE_AISDBHOST=localhost \
VITE_AISDBPORT=9924 \
npx vite build --outDir "${MAPDIR}/../dist_map"
#zip -ru9 aisdb_web/dist_map.zip aisdb_web/dist_map/

VITE_BINGMAPTILES=1 \
VITE_TILESERVER=aisdb.meridian.cs.dal.ca \
VITE_DISABLE_SSL_DB=1 \
VITE_DISABLE_STREAM=1 \
VITE_AISDBHOST=localhost \
VITE_AISDBPORT=9924 \
npx vite build --outDir "${MAPDIR}/../dist_map_bingmaps"
#zip -ru9 aisdb_web/dist_map_bingmaps.zip aisdb_web/dist_map_bingmaps/
