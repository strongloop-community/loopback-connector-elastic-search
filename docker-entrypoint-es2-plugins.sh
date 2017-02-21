#!/bin/bash
# setting up prerequisites

# re-runs will give an error that is harmless:
#   > ERROR: plugin directory /usr/share/elasticsearch/plugins/delete-by-query already exists.
#   > To update the plugin, uninstall it first using 'remove delete-by-query' command
#plugin install delete-by-query

# https://github.com/mobz/elasticsearch-head/#running-as-a-plugin-of-elasticsearch
plugin install mobz/elasticsearch-head


# access it at /_plugin/elasticsearch-inquisitor/
plugin install polyfractal/elasticsearch-inquisitor

#exec /docker-entrypoint.sh elasticsearch
exec elasticsearch -Des.insecure.allow.root=true
