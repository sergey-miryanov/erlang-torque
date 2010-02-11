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
-export ([job_stat/2, queue_stat/2]).

%% Internal
-export ([control/3]).

-define ('CMD_CONNECT',           0).
-define ('CMD_STAT_JOB',          1).
-define ('CMD_STAT_QUEUE',        2).
-define ('CMD_STAT_JOB_A',        3).
-define ('CMD_STAT_QUEUE_A',      4).

-define ('ATTR_EXECUTION_TIME',   "1").
-define ('ATTR_ACCOUNT_NAME',     "2").
-define ('ATTR_CHECKPOINT',       "3").
-define ('ATTR_ERROR_PATH',       "4").
-define ('ATTR_GROUP_LIST',       "5").
-define ('ATTR_HOLD_TYPES',       "6").
-define ('ATTR_JOIN_PATHS',       "7").
-define ('ATTR_KEEP_FILES',       "8").
-define ('ATTR_RESOURCE_LIST',    "9").
-define ('ATTR_MAIL_POINTS',      "10").
-define ('ATTR_MAIL_USERS',       "11").
-define ('ATTR_JOB_NAME',         "12").
-define ('ATTR_OUTPUT_PATH',      "13").
-define ('ATTR_PRIORITY',         "14").
-define ('ATTR_DESTINATION',      "15").
-define ('ATTR_RERUNABLE',        "16").
-define ('ATTR_SESSION_ID',       "17").
-define ('ATTR_SHELL_PATH_LIST',  "18").
-define ('ATTR_USER_LIST',        "19").
-define ('ATTR_VARIABLE_LIST',    "20").
-define ('ATTR_CREATE_TIME',      "21").
-define ('ATTR_DEPEND',           "22").
-define ('ATTR_MODIF_TIME',       "23").
-define ('ATTR_QUEUE_TIME',       "24").
-define ('ATTR_QUEUE_TYPE',       "25").
-define ('ATTR_STAGEIN',          "26").
-define ('ATTR_STAGEOUT',         "27").
-define ('ATTR_JOB_STATE',        "28").

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

job_stat (JobID, Attrs) when is_list (JobID) and is_list (Attrs) ->
  gen_server:call (torque, {job_stat_a, JobID, Attrs}).

queue_stat (QueueID, Attrs) when is_list (QueueID) and is_list (Attrs) ->
  gen_server:call (torque, {queue_stat_a, QueueID, Attrs}).

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
          {stop, string:join (["Error connecting to server: ", Error], "")}
      end;
    {error, Error} ->
      {stop, string:join (["Error loading torque driver: ", erl_ddll:format_error (Error)], "")}
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
handle_call ({job_stat_a, JobID, Attrs}, _From, #state {port = Port} = State) ->
  List = attr_atom_to_str (Attrs),
  Count = erlang:integer_to_list (length (List)),
  Buffer = string:join ([JobID | [Count | List]], ","),
  Reply = torque:control (Port, ?CMD_STAT_JOB_A, erlang:list_to_binary (Buffer)),
  {reply, Reply, State};
handle_call ({queue_stat_a, QueueID, Attrs}, _From, #state {port = Port} = State) ->
  List = attr_atom_to_str (Attrs),
  Count = erlang:integer_to_list (length (List)),
  Buffer = string:join ([QueueID | [Count | List]], ","),
  Reply = torque:control (Port, ?CMD_STAT_QUEUE_A, erlang:list_to_binary (Buffer)),
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

attr_atom_to_str (List) ->
  attr_atom_to_str (List, []).

attr_atom_to_str ([exec_time        | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_EXECUTION_TIME | L]);
attr_atom_to_str ([account_name     | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_ACCOUNT_NAME | L]);
attr_atom_to_str ([checkpoint       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_CHECKPOINT | L]);
attr_atom_to_str ([error_path       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_ERROR_PATH | L]);
attr_atom_to_str ([group_list       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_GROUP_LIST | L]);
attr_atom_to_str ([hold_types       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_HOLD_TYPES | L]);
attr_atom_to_str ([join_paths       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_JOIN_PATHS | L]);
attr_atom_to_str ([keep_files       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_KEEP_FILES | L]);
attr_atom_to_str ([resource_list    | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_RESOURCE_LIST | L]);
attr_atom_to_str ([mail_points      | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_MAIL_POINTS | L]);
attr_atom_to_str ([mail_users       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_MAIL_USERS | L]);
attr_atom_to_str ([job_name         | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_JOB_NAME | L]);
attr_atom_to_str ([output_path      | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_OUTPUT_PATH | L]);
attr_atom_to_str ([priority         | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_PRIORITY | L]);
attr_atom_to_str ([destination      | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_DESTINATION | L]);
attr_atom_to_str ([rerunable        | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_RERUNABLE | L]);
attr_atom_to_str ([session_id       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_SESSION_ID | L]);
attr_atom_to_str ([shell_path_list  | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_SHELL_PATH_LIST | L]);
attr_atom_to_str ([user_list        | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_USER_LIST | L]);
attr_atom_to_str ([variable_list    | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_VARIABLE_LIST | L]);
attr_atom_to_str ([create_time      | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_CREATE_TIME | L]);
attr_atom_to_str ([ctime            | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_CREATE_TIME | L]);
attr_atom_to_str ([depend           | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_DEPEND | L]);
attr_atom_to_str ([modif_time       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_MODIF_TIME | L]);
attr_atom_to_str ([mtime            | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_MODIF_TIME | L]);
attr_atom_to_str ([queue_time       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_QUEUE_TIME | L]);
attr_atom_to_str ([qtime            | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_QUEUE_TIME | L]);
attr_atom_to_str ([queue_type       | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_QUEUE_TYPE | L]);
attr_atom_to_str ([stagein          | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_STAGEIN | L]);
attr_atom_to_str ([stageout         | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_STAGEOUT | L]);
attr_atom_to_str ([job_state        | Tail], L) -> attr_atom_to_str (Tail, [?ATTR_JOB_STATE | L]);
attr_atom_to_str ([], L) ->
  L.

