-module (torque).
-author ('sergey.miryanov@gmail.com').

-export ([start_link/1]).

%% gen_server
-export ([init/1]).

-record (state, {port}).

start_link (Server) ->
  gen_server:start_link ({local, torque}, ?MODULE, Server, []).

%% gen_server

init (Server) ->
  process_flag (trap_exit, true),
  SearchDir = filename:join ([filename:dirname (code:which (?MODULE)), "..", "ebin"]),
  case erl_ddll:load (SearchDir, "torque_drv")
  of
    ok ->
      Port = open_port ({spawn, string:join (["torque_drv", Server], " ")}, [binary]),
      {ok, #state {port = Port}};
    {error, Error} ->
      io:format ("Error loading torque driver: ~p~n", [erl_ddll:format_error (Error)]),
      {stop, failed}
  end.
