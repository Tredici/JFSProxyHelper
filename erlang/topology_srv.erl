-module(topology_srv).
-behaviour(gen_server).

-record(datanode, {id,ts,startTS,r,status,deps,meps,statusTS,topologyTS,fileTS}).

%% API
-export([start/1, stop/1, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-record(state, {table}).

-export([expires/1]).

expires(Name) ->
    receive
        after 1000 -> ok
    end,
    Now = erlang:system_time(millisecond),
    ExTime = Now - 15 * 1000,
    gen_server:cast(Name, {expire, ExTime}),
    expires(Name).

% argument is the Atom name of the 
start(Name) ->
    SrvName = my_supervisor:start_child(Name),
    spawn(?MODULE, expires, [SrvName]).

stop(Name) ->
    gen_server:call(Name, stop).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

init(_Args) ->
    mnesia:start(),
    % create table if not exists
    case mnesia:create_table(datanode, [
        {ram_copies, [node()]},
        {record_name, datanode},
        {attributes, record_info(fields, datanode)}
    ]) of
        {atomic,ok} -> io:format("mnesia table created~n", []);
        _ -> io:format("Failed to create mnesia table, abort.~n", []), halt()
    end,
    {ok, #state{table=datanode}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(snapshot, _From, State) ->
    CatchAll = [{'_',[],['$_']}],
    All = mnesia:dirty_select(State#state.table, CatchAll),
    Res = lists:map(
        fun(Val) ->
            {
                Val#datanode.id,
                Val#datanode.ts,
                Val#datanode.startTS,
                Val#datanode.r,
                Val#datanode.status,
                Val#datanode.deps,
                Val#datanode.meps,
                Val#datanode.statusTS,
                Val#datanode.topologyTS,
                Val#datanode.fileTS
            }
        end,
        All),
    {reply, Res, State};

handle_call(endpoints, _From, State) ->
    CatchAllDe = [{
            #datanode{
            deps = '$1',
            _ = '_'
            },
            [],
            ['$1']}],
    All = mnesia:dirty_select(State#state.table, CatchAllDe),
    Res = lists:append(All),
    {reply, Res, State};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({update,
    {Id,TS,StartTS,R,Status,DEPs,MEPs,
    StatusTS,TopologyTS,FileTS}
    }, State) ->
    Datanode = #datanode{
        id = Id,
        ts = TS,
        startTS = StartTS,
        r = R,
        status = Status,
        deps = DEPs,
        meps = MEPs,
        statusTS = StatusTS,
        topologyTS = TopologyTS,
        fileTS = FileTS
    },
    Fun = fun() ->
            mnesia:write(Datanode)
        end,
    mnesia:transaction(Fun),
    {noreply, State};

handle_cast({delete,{Id,StartTS}}, State) ->
    Fun = fun() ->
        % TODO: fix deletion
        CatchExpired = [{
            #datanode{
            id = Id,
            startTS = StartTS,
            _ = '_'
            }, [], ['$_']}],
        case mnesia:select(State#state.table, CatchExpired) of
            [H|_] ->
                io:format("Delete: ~w~n", [H]),
                mnesia:delete({State#state.table, Id});
            _ -> no
        end
    end,
    mnesia:transaction(Fun),
    {noreply, State};

handle_cast({expire,TS}, State) ->
    Fun = fun() ->
        io:format("Expiration of uploaded before: ~w~n", [TS]),
        CatchLower = [{
            #datanode{
            id = '$1',
            ts = '$2',
            _ = '_'
            },
            [{'<', '$2', TS}],
            ['$1']}],
        Ans = mnesia:select(State#state.table, CatchLower),
        lists:map(fun(Key) ->
            mnesia:delete({State#state.table, Key})
        end, Ans)
    end,
    mnesia:transaction(Fun),
    {noreply, State};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
