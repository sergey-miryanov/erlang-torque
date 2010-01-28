-module (torque).
-author ('sergey.miryanov@gmail.com').

-behaviour (gen_server).

-export ([start_link/1]).

%% gen_server
-export ([init/1, handle_call/3]).
-export ([handle_cast/2, handle_info/2,
    terminate/2, code_change/3]).

%% API
-export ([connect/1]).
-export ([job_stat/1, queue_stat/1, server_stat/0]).

%% Internal
-export ([control/3]).

-define ('CMD_STAT_JOB',    1).
-define ('CMD_STAT_QUEUE',  2).
-define ('CMD_CONNECT',     3).

-record (state, {port}).

start_link (Server) ->
  gen_server:start_link ({local, torque}, ?MODULE, Server, []).

connect (Server) ->
  gen_server:call (torque, {connect, Server}).

job_stat (JobID) when is_list (JobID) ->
  gen_server:call (torque, {job_stat, JobID}).

queue_stat (QueueID) when is_list (QueueID) ->
  gen_server:call (torque, {queue_stat, QueueID}).

server_stat () ->
  gen_server:call (torque, {queue_stat, ""}).

%% gen_server

init (Server) ->
  process_flag (trap_exit, true),
  SearchDir = filename:join ([filename:dirname (code:which (?MODULE)), "..", "ebin"]),
  case erl_ddll:load (SearchDir, "torque_drv")
  of
    ok ->
      Port = open_port ({spawn, "torque_drv"}, [binary]),
      case torque:control (Port, ?CMD_CONNECT, erlang:list_to_binary (Server))
      of
        {ok} ->
          {ok, #state {port = Port}};
        {error, Error} ->
          io:format ("Error connecting to server: ~p~n", [Error]),
          {stop, failed}
      end;
    {error, Error} ->
      io:format ("Error loading torque driver: ~p~n", [erl_ddll:format_error (Error)]),
      {stop, failed}
  end.

code_change (_OldVsn, State, _Extra) ->
  {ok, State}.

handle_cast (_Msg, State) ->
  {noreply, State}.

handle_info (_Info, State) ->
  {noreply, State}.

terminate (normal, #state {port = Port}) ->
  port_command (Port, term_to_binary ({close, nop})),
  port_close (Port),
  ok;
terminate (_Reason, _State) ->
  ok.

handle_call ({job_stat, JobID}, _From, #state {port = Port} = State) ->
  Reply = torque:control (Port, ?CMD_STAT_JOB, erlang:list_to_binary (JobID)),
  {reply, Reply, State};
handle_call ({queue_stat, QueueID}, _From, #state {port = Port} = State) ->
  Reply = torque:control (Port, ?CMD_STAT_QUEUE, erlang:list_to_binary (QueueID)),
  {reply, Reply, State};
handle_call ({connect, Server}, _From, #state {port = Port} = State) ->
  Reply = torque:control (Port, ?CMD_CONNECT, erlang:list_to_binary (Server)),
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

