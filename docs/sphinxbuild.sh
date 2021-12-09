SCRIPTPATH="$( cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
RSTSOURCEDIR="${SCRIPTPATH}/source"
HTMLOUTPUTDIR="${SCRIPTPATH}/html"
PKGDIR="${SCRIPTPATH}/../aisdb"
ROOTDIR="${SCRIPTPATH}/../"

#sphinx-apidoc --module-first --separate --implicit-namespaces -o "${RSTSOURCEDIR}" "${PKGDIR}"

[[ -d "${RSTSOURCEDIR}/sphinx-apidoc" ]] && rm ${RSTSOURCEDIR}/sphinx-apidoc/*
sphinx-apidoc --separate --force --implicit-namespaces --module-first -q --no-toc -o "${RSTSOURCEDIR}/sphinx-apidoc" "${PKGDIR}"
pandoc "${ROOTDIR}/readme.md" --from markdown --to rst -s -o "${RSTSOURCEDIR}/sphinx-apidoc/readme.rst"
sphinx-build -a -j auto -q -b=html "${RSTSOURCEDIR}" "${HTMLOUTPUTDIR}"
