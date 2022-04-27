SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
ROOTDIR="${SCRIPTPATH}"
PKGDIR="${SCRIPTPATH}/aisdb"
RSTSOURCEDIR="${SCRIPTPATH}/docs/source"
MAPDIR="${SCRIPTPATH}/aisdb_web/map"
SPHINXDIR="${SCRIPTPATH}/aisdb_web/dist_sphinx"
CARGODIR="${SCRIPTPATH}/aisdb_web/dist_cargodoc"
WASMDIR="${SCRIPTPATH}/aisdb_wasm"


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

cp "$ROOTDIR/readme.rst" "${RSTSOURCEDIR}/readme.rst"
cp "$ROOTDIR/changelog.rst" "${RSTSOURCEDIR}/changelog.rst"
sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -o "${RSTSOURCEDIR}/api" "${PKGDIR}"
python -m sphinx -a -j auto -q -b=html "${RSTSOURCEDIR}" "${SPHINXDIR}"
cp "${RSTSOURCEDIR}/scriptoutput.png" "$SPHINXDIR/_images/"


#cp -r "${ROOTDIR}/aisdb_web/public" "${SPHINXDIR}/public"
#cp -r "${MAPDIR}" "${SPHINXDIR}/map"

#sed -i 's/<script /<script type="module" /g' "${SPHINXDIR}/index.html"
#sed -i 's/"_static\//"\/_static\//g' "${SPHINXDIR}/index.html"
#sed -i 's/<\/head>/<script src="https:\/\/code.jquery.com\/jquery-3.6.0.min.js"><\/script><\/head>/g' "${SPHINXDIR}/index.html"

cd "${WASMDIR}"
wasm-pack build --target web --out-dir "${MAPDIR}/pkg" --release
wasm-opt -O3 -o "${MAPDIR}/pkg/client_bg.wasm" "${MAPDIR}/pkg/client_bg.wasm"
cd "${ROOTDIR}"

