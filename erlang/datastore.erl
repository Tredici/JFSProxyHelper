-module(datastore).
-export([start/2, listener/1]).

%{<9232.1.0>,update,
%               {1,1694971027362,1694971023692,3,"RUNNING",
%                ["http://127.0.0.1:8888"],
%                ["http://127.0.0.1:8080"],
%                1694971023692,1694971023692,1694971023692}}


handleVc(Server,{Id,TS,StartTS,R,Status,DEPs,MEPs,
            StatusTS,TopologyTS,FileTS}) when 
        is_integer(Id), is_integer(TS),
        is_integer(StartTS), is_integer(R),
        is_integer(StatusTS), is_integer(TopologyTS),
        is_integer(FileTS),
        is_list(Status), is_list(DEPs), is_list(MEPs)
        ->
    % update for running?
    case Status of
        "RUNNING" ->
            % inform db
            gen_server:cast(Server, {update, 
                {Id,TS,StartTS,R,Status,DEPs,MEPs,
                StatusTS,TopologyTS,FileTS}
                });
        _ -> gen_server:cast(Server, {delete,{Id,StartTS}})
    end;
handleVc(_,_) -> ignore.

requestSnapshot(Server,Pid) ->
    Pid ! gen_server:call(Server, snapshot).

listener(Server) ->
    receive
        M -> io:format("Received Msg: ~p~n", [M])
    end,
    case M of
        {_,update,VC} ->
            handleVc(Server, VC);
        {Pid, snapshotReq} ->
            requestSnapshot(Server, Pid);
        % ignore
        _ -> ignore
    end,
    listener(Server).


% start listener
start(Name, Server) ->
    Pid = spawn(?MODULE, listener, [Server]),
    erlang:register(Name, Pid).
