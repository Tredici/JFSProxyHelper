-module(url_srv).
-behaviour(gen_server).

%% API
-export([start/1, stop/1, start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
% urls: list of urls for redirect
% n: size of the list
% pos: index of the item to be supplied
-record(state, {urls,n,pos}).

-export([reload/1]).
% periodically reload
reload(Name) ->
    receive
        after 1000 -> ok
    end,
    gen_server:cast(Name, reload),
    reload(Name).

start(Name) ->
    start_link(Name),
    % periodically trigger reload
    spawn(?MODULE, reload, [Name]).

stop(Name) ->
    gen_server:call(Name, stop).

start_link(Name) ->
    gen_server:start_link({local, Name}, ?MODULE, [], []).

init(_Args) ->
    {ok, #state{urls=[], n=0, pos=0}}.

handle_call(stop, _From, State) ->
    {stop, normal, stopped, State};

handle_call(url, _From, State) ->
    Ans = case State#state.n of
        0 -> NewState = State, no;
        _ -> Pos = State#state.pos rem State#state.n,
            NewPos = (Pos + 1) rem State#state.n,
            NewState = State#state{
                pos = NewPos
            },
            lists:nth(Pos+1, State#state.urls)
    end,
    {reply, Ans, NewState};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.


handle_cast(reload, State) ->
    Eps = gen_server:call(dataserver, endpoints),
    NewState = State#state{
        urls = Eps,
        n=length(Eps)
    },
    {noreply, NewState};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.




