-module(main).
-export([start/0, start/1]).

% start erlang http proxy
start([Port]) when is_atom(Port) ->
    start(list_to_integer(atom_to_list(Port)));
start([Port]) when is_integer(Port) ->
    start(Port);
start(Port) when is_list(Port) ->
    start(list_to_integer(Port));
start(Port) when is_integer(Port) ->
    % start topology mnesia datastore
    topology_srv:start(datastore),
    % start url server
    url_srv:start(urlserver),
    erlproxy:init(Port),
    ok.

start() ->
    start([10000]).
