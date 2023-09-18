-module(erlproxy).
-export([init/1, init/0, listener/1, client/1]).

% Simplest possible HTTP erlang proxy
% used to redirect client HTTP GET Reqs to Datanode
% Obtain datanodes Ip via mnesia via urlserver


% peek random supplier
peek_random_supplier(_) ->
    gen_server:call(urlserver, url).

% Consume (discard) all headers
consume_http_headers(ClientSocket) ->
    case gen_tcp:recv(ClientSocket, 0) of
        % new line of header
        {ok, {http_header, _, _, _, _} } ->
            consume_http_headers(ClientSocket);
        % end of header
        {ok,http_eoh} ->
            ok
    end.


% line by line, send whole header
send_all_http_headers(ClientSocket, []) ->
    gen_tcp:send(ClientSocket, "\r\n" ++ []);
send_all_http_headers(ClientSocket, [Header|Headers]) ->
    { http_header, _, _, Key, Value } = Header,
    gen_tcp:send(ClientSocket,
        Key ++ ": " ++ Value ++ "\r\n"
    ),
    send_all_http_headers(ClientSocket, Headers).

% send http_response and close the connection
send_http_response(ClientSocket, H, HeaderList, Body) ->
    % send first line
    {http_response, {V1,V2}, Code, Msg} = H,
    gen_tcp:send(ClientSocket,
        "HTTP/" ++ integer_to_list(V1) ++ "." ++
        integer_to_list(V2) ++ " " ++ 
        integer_to_list(Code) ++ " " ++ Msg ++ "\r\n"
    ),
    send_all_http_headers(ClientSocket, HeaderList),
    case Body of
        % empty body
        [] -> ok;
        % non empty body, send it as is
        [_|_] -> gen_tcp:send(ClientSocket, Body)
    end,
    gen_tcp:close(ClientSocket).

redirect_to_datanode(ClientSocket, Supplier, Path) ->
    Location = Supplier ++ Path,
    H = {http_response, {1,1}, 308, "Permanent Redirect"},
    Headers = [
        {
            http_header,
            0,
            'Location',
            "Location",
            Location
        },
        {
            http_header,
            38,
            'Content-Length',
            "Content-Length",
            "0"
        }
    ],
    Body = "",
    send_http_response(ClientSocket, H, Headers, Body).

send_503(ClientSocket) -> 
    H = {http_response, {1,1}, 503, "Service Unavailable"},
    Headers = [
        {
            http_header,
            38,
            'Content-Length',
            "Content-Length",
            "0"
        }
    ],
    Body = "",
    send_http_response(ClientSocket, H, Headers, Body).

% redirect GET request
handle_redirect(ClientSocket, Path) ->
    % consume request header
    ok = consume_http_headers(ClientSocket),
    % find supplier for redirection
    case peek_random_supplier(Path) of
        no -> send_503(ClientSocket);
        Supplier -> redirect_to_datanode(ClientSocket, Supplier, Path)
    end.

% send all response headers

% Only GET method is supported
handle_method_not_supported(ClientSocket, Method) ->
    H = {http_response, {1, 1}, 404, "Method Not Allowed"},
    Body = "Unsupported method " ++ Method ++ "\nOnly GET is allowed.\n",
    Headers = [
        {
            http_header,
            38,
            'Content-Length',
            "Content-Length",
            integer_to_list(erlang:length(Body))
        }
    ],
    send_http_response(ClientSocket, H, Headers, Body).

handle_version_not_supported(ClientSocket, Version) ->
    {V1, V2} = Version,
    H = {http_response, {1, 1}, 505, "HTTP Version Not Supported"},
    Body = "Unsupported method " ++ integer_to_list(V1) ++ "."
            ++ integer_to_list(V2) ++ "\nOnly GET is allowed.\n",
    Headers = [
        {
            http_header,
            38,
            'Content-Length',
            "Content-Length",
            integer_to_list(erlang:length(Body))
        }
    ],
    send_http_response(ClientSocket, H, Headers, Body).

client(ClientSocket) ->
    % get request, asset http GET request
    case gen_tcp:recv(ClientSocket, 0) of
        {ok,{http_request,'GET',{abs_path,Path},{1,1}}} ->
            handle_redirect(ClientSocket, Path);
        {ok,{http_request,Method,{abs_path,_},{1,1}}} ->
            handle_method_not_supported(ClientSocket, Method);
        {ok,{http_request,_,{abs_path,_},Version}} ->
            handle_version_not_supported(ClientSocket, Version)
    end,
    ok.

% server loop
listener(ServerPort) when is_integer(ServerPort) ->
    % open listening socket
    {ok, ServerSocket} = gen_tcp:listen(ServerPort, [
        {packet, http},
        {active, false},
        {exit_on_close, true},
        {reuseaddr,true}
    ]),
    io:format("Proxy listening on port: ~w~n", [ServerPort]),
    listener(ServerSocket);
listener(ServerSocket) ->
    % receive client request
    case gen_tcp:accept(ServerSocket) of
        {ok, S} ->
            % spawn client handler
            spawn(?MODULE, client, [S]),
            % wait for new connections
            listener(ServerSocket);
        M -> io:format("Error listening socket: ~w~n", [M]),
            halt()
    end.

init(ServerPort) ->
    % start server
    spawn(?MODULE, listener, [ServerPort]).

init() -> init(9876).
