SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
RSTSOURCEDIR="${SCRIPTPATH}/source"
HTMLOUTPUTDIR="${SCRIPTPATH}/html"
PKGDIR="${SCRIPTPATH}/../aisdb"
ROOTDIR="${SCRIPTPATH}/.."


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
#[[ ! -z `ls -A "${HTMLOUTPUTDIR}/rust"` ]] && >&2 echo 'ensure that rust docs are built!' && exit 1

cp "$ROOTDIR/readme.rst" "${RSTSOURCEDIR}/readme.rst"
sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -o "${RSTSOURCEDIR}/api" "${PKGDIR}"
python -m sphinx -a -j auto -q -b=html "${RSTSOURCEDIR}" "${HTMLOUTPUTDIR}"
cp "${RSTSOURCEDIR}/db_schema.png" "$HTMLOUTPUTDIR/_images/"
cp "${RSTSOURCEDIR}/scriptoutput.png" "$HTMLOUTPUTDIR/_images/"
