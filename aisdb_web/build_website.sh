#!/bin/bash
SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
ROOTDIR="${SCRIPTPATH}/.."
PKGDIR="${ROOTDIR}/aisdb"
RSTSOURCEDIR="${ROOTDIR}/docs/source"
MAPDIR="${ROOTDIR}/aisdb_web/map"
SPHINXDIR="${ROOTDIR}/aisdb_web/dist_sphinx"
#JSDOCDIR="${ROOTDIR}/aisdb_web/dist_jsdoc"
WASMDIR="${ROOTDIR}/aisdb_wasm"

set -e

# jsdocs site build
#cd "${SCRIPTPATH}"
#npx jsdoc \
#  --recurse "${MAPDIR}" \
#  --package "${ROOTDIR}/aisdb_web/package.json" \
#  --destination "${JSDOCDIR}"


# webassembly components build for map
cd "${WASMDIR}"
wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --release
wasm-opt -O3 -o "${MAPDIR}/pkg/client_bg.wasm" "${MAPDIR}/pkg/client_bg.wasm"
#wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --dev


# build map webapp
cd "${MAPDIR}" 
  VITE_AISDBHOST=$AISDBHOST \
  VITE_AISDBPORT=$AISDBPORT \
  VITE_BINGMAPTILES=$VITE_BINGMAPTILES \
  npx vite build --outDir "${MAPDIR}/../dist_map"
  #VITE_BINGMAPTILES="`if [ -z ${BINGMAPSKEY} ]; then echo ''; else echo 1; fi`" \

