-module (torque).
-author ('sergey.miryanov@gmail.com').

-export ([start_link/1]).

%% gen_server
-export ([init/1, handle_call/3]).

%% API
-export ([job_stat/1]).

%% Internal
-export ([control/3]).

-define ('CMD_STAT_JOB', 1).

-record (state, {port}).

start_link (Server) ->
  gen_server:start_link ({local, torque}, ?MODULE, Server, []).

job_stat (JobID) when is_list (JobID) ->
  gen_server:call (torque, {job_stat, JobID}).

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

handle_call ({job_stat, JobID}, _From, #state {port = Port} = State) ->
  Reply = torque:control (Port, ?CMD_STAT_JOB, erlang:list_to_binary (JobID)),
  {reply, Reply, State};
handle_call (Request, _From, State) ->
  {reply, {unknown, Request}, State}.

control (Port, Command, Data) 
  when is_port (Port) and is_integer (Command) and is_binary (Data) ->
    port_control (Port, Command, Data),
    wait_result (Port).

wait_result (_Port) ->
  receive
	  Smth -> Smth
  end.
