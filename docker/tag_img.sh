echo "GIT CONTEXT:\n"
find .. | grep -vi "`cat ../.gitignore`" | grep -vi '\/.git\/'
echo "\n\nCAUTION: TAGGING PUBLIC IMAGE WITH BUILD CONTEXT: \n"
find .. | grep -vi "`cat ../.dockerignore`"
docker build --target aisdb --tag meridiancfi/aisdb:latest --compress --file ./Dockerfile ..
docker push meridiancfi/aisdb:latest
