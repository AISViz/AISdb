SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
ROOTDIR="${SCRIPTPATH}/.."
PKGDIR="${ROOTDIR}/aisdb"
RSTSOURCEDIR="${ROOTDIR}/docs/source"
MAPDIR="${ROOTDIR}/aisdb_web/map"
SPHINXDIR="${ROOTDIR}/aisdb_web/dist_sphinx"
CARGODIR="${ROOTDIR}/aisdb_web/dist_cargodoc"
JSDOCDIR="${ROOTDIR}/aisdb_web/dist_jsdoc"
WASMDIR="${ROOTDIR}/aisdb_wasm"


# cargo docs site build
rm -rf "$SPHINXDIR"
mkdir -p "${RSTSOURCEDIR}/api"
mkdir -p "${SPHINXDIR}/_images"
[[ ! -z `ls -A "${RSTSOURCEDIR}/api"` ]] && rm ${RSTSOURCEDIR}/api/*
cargo doc \
  --document-private-items \
  --manifest-path="$ROOTDIR/aisdb_rust/Cargo.toml" \
  --no-deps \
  --package=aisdb \
  --release \
  --target-dir="${CARGODIR}"


# sphinx docs site build
cp "$ROOTDIR/readme.rst" "${RSTSOURCEDIR}/readme.rst"
cp "$ROOTDIR/changelog.rst" "${RSTSOURCEDIR}/changelog.rst"
sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -o "${RSTSOURCEDIR}/api" "${PKGDIR}"
python -m sphinx -a -j auto -q -b=html "${RSTSOURCEDIR}" "${SPHINXDIR}"
cp "${RSTSOURCEDIR}/scriptoutput.png" "$SPHINXDIR/_images/"


# jsdocs site build
npx jsdoc \
  --recurse "${MAPDIR}" \
  --package "${ROOTDIR}/aisdb_web/package.json" \
  --destination "${JSDOCDIR}"


# webassembly components build for map
cd "${WASMDIR}"
wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --release
wasm-opt -O3 -o "${MAPDIR}/pkg/client_bg.wasm" "${MAPDIR}/pkg/client_bg.wasm"


# build map webapp
cd "${MAPDIR}" 
VITE_BINGMAPSKEY=$BINGMAPSKEY \
  VITE_AISDBHOST=$AISDBHOST \
  VITE_AISDBPORT=$AISDBPORT \
  npx vite build --outDir "${MAPDIR}/../dist_map"
