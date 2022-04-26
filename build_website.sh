SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
RSTSOURCEDIR="${SCRIPTPATH}/docs/source"
HTMLOUTPUTDIR="${SCRIPTPATH}/aisdb_web/dist_sphinx"
PKGDIR="${SCRIPTPATH}/aisdb"
ROOTDIR="${SCRIPTPATH}/"
MAPDIR="${SCRIPTPATH}/aisdb_web/map"
WASMDIR="${SCRIPTPATH}/aisdb_wasm"


rm -rf "$HTMLOUTPUTDIR"
mkdir -p "${RSTSOURCEDIR}/api"
mkdir -p "${HTMLOUTPUTDIR}/_images"
[[ ! -z `ls -A "${RSTSOURCEDIR}/api"` ]] && rm ${RSTSOURCEDIR}/api/*
cargo doc \
  --document-private-items \
  --manifest-path="$ROOTDIR/aisdb_rust/Cargo.toml" \
  --no-deps \
  --package=aisdb \
  --release \
  --target-dir="$HTMLOUTPUTDIR/rust"

cp "$ROOTDIR/readme.rst" "${RSTSOURCEDIR}/readme.rst"
cp "$ROOTDIR/changelog.rst" "${RSTSOURCEDIR}/changelog.rst"
sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -o "${RSTSOURCEDIR}/api" "${PKGDIR}"
python -m sphinx -a -j auto -q -b=html "${RSTSOURCEDIR}" "${HTMLOUTPUTDIR}"
cp "${RSTSOURCEDIR}/scriptoutput.png" "$HTMLOUTPUTDIR/_images/"
cp -r "${SCRIPTPATH}/aisdb_web/public" "${HTMLOUTPUTDIR}/public"
cp -r "${MAPDIR}" "${HTMLOUTPUTDIR}/map"

sed -i 's/<script /<script type="module" /g' "${HTMLOUTPUTDIR}/index.html"
sed -i 's/"_static\//"\/_static\//g' "${HTMLOUTPUTDIR}/index.html"
#sed -i 's/<\/head>/<script src="https:\/\/code.jquery.com\/jquery-3.6.0.min.js"><\/script><\/head>/g' "${HTMLOUTPUTDIR}/index.html"

cd "${WASMDIR}"
wasm-pack build --target web --out-dir "${HTMLOUTPUTDIR}/pkg" --release
wasm-opt -O3 -o "${HTMLOUTPUTDIR}/pkg/client_bg.wasm" "${HTMLOUTPUTDIR}/pkg/client_bg.wasm"

