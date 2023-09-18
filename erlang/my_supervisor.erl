-module(my_supervisor).
-export([start_child/1, super/1]).

% process monitoring server
loop(Name) ->
    io:format("Starting server ~w...~n", [Name]),
    topology_srv:start_link(Name),
    io:format("Server ~w started!~n", [Name]),
    receive
        {'EXIT',FromPID,Reason} ->
            io:format("Exit from ~w for ~w~n", [FromPID,Reason]),
            io:format("Restarting..."),
            loop(Name);
        M -> io:format("Received: ~p, terminating...~n", [M])
    end.
super(Name) ->
    process_flag(trap_exit, true),
    loop(Name).


start_child(Name) ->
    % start server
    erlang:spawn(?MODULE, super, [dataserver]),
    % start topology listener
    datastore:start(Name, dataserver),
    dataserver.

