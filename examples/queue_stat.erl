#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname queue_stat

print_stat (Stat) ->
  {Name, Attr} = Stat,
  io:format ("~p~n", [Name]).

main (_) ->

  torque:start_link ("casper"),

  io:format ("Name of each job at the server: ~n"),
  {ok, List} = torque:queue_stat (""),
  lists:foreach (fun (A) -> print_stat (A) end, List),

  io:format ("Name of each job at the 'batch' queue: ~n"),
  {ok, List} = torque:queue_stat ("batch"),
  lists:foreach (fun (A) -> print_stat (A) end, List),

  ok.
