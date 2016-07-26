#!/bin/bash
# setting up prerequisites

if [ -f server/datasources.bak ]; then
  cp server/datasources.bak server/datasources.json
else
  cp server/datasources.json server/datasources.bak
fi
if [ -f server/model-config.bak ]; then
  cp server/model-config.bak server/model-config.json
else
  cp server/model-config.json server/model-config.bak
fi
sed -i 's/hosted\.foundcluster\.com/es/g; s/9243/9200/g' /apps/examples/server/datasources.json
sed -i 's/elasticsearch-ssl/elasticsearch-plain/g' /apps/examples/server/model-config.json
sed -i '21,41d; s/"requestTimeout": 30000/"requestTimeout": 30000,/' /apps/examples/server/datasources.json
if [ -d node_modules ]; then
  rm -rf node_modules
fi
npm install
curl -X POST http://es:9200/shakespeare
exec "$@"
