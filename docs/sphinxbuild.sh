SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
RSTSOURCEDIR="${SCRIPTPATH}/source"
HTMLOUTPUTDIR="${SCRIPTPATH}/html"
PKGDIR="${SCRIPTPATH}/../aisdb"
ROOTDIR="${SCRIPTPATH}/.."


mkdir -p "${RSTSOURCEDIR}/sphinx-apidoc"
rm -rf "$HTMLOUTPUTDIR"
[[ ! -z `ls -A "${RSTSOURCEDIR}/sphinx-apidoc"` ]] && rm ${RSTSOURCEDIR}/sphinx-apidoc/*
#pandoc "${ROOTDIR}/readme.md" --from markdown --to rst -s -o "${RSTSOURCEDIR}/sphinx-apidoc/readme.rst"
cp "$ROOTDIR/readme.rst" "${RSTSOURCEDIR}/readme.rst"
sphinx-apidoc --separate --force --implicit-namespaces --module-first --no-toc -q -o "${RSTSOURCEDIR}/sphinx-apidoc" "${PKGDIR}"
python -m sphinx -a -j auto -q -b=html "${RSTSOURCEDIR}" "${HTMLOUTPUTDIR}"
