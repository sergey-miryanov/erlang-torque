#!/usr/bin/env escript
%%! -pa ../ebin -pa ebin -sname job_stat

print_stat (Stat) ->
  if 
  (length (Stat) == 2) -> 
    [Name | Value] = Stat, 
    io:format ("name: ~p, value = ~p~n", [Name, Value]);
  (length (Stat) == 3) ->
    [Name | [Resource | Value]] = Stat,
    io:format ("name: ~p, value = ~p, resource: ~p~n", [Name, Value, Resource])
  end.

main (_) ->

  torque:start_link ("casper"),
  {ok, List} = torque:job_stat ("70.casper"),
  lists:foreach (fun (A) -> print_stat (A) end, List),

  ok.
