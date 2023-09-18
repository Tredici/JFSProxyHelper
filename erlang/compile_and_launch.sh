#!/bin/bash

cd $(dirname "$0")
erlc my_supervisor.erl
erlc topology_srv.erl
erlc datastore.erl
erlc url_srv.erl
erlc erlproxy.erl
erlc main.erl

echo "$@"
erl -s main start "$@" \
    -setcookie biscotto \
    -name b@127.0.0.1
#    -noshell \

