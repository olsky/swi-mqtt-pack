:- use_module(library(mqtt)).


versions:-
  mqtt_version(MqttVerMa, MqttVerMi, MqttVerRe),
  pack_version(PackVerMa, PackVerMi, PackVerRe),
  format('% Pack version: ~w.~w.~w Mosquitto version: ~w.~w.~w~n', [PackVerMa, PackVerMi, PackVerRe, MqttVerMa, MqttVerMi, MqttVerRe]),
  true.




/*



*/
timer_pub(Topic, Value) :-
  pub_connection(A),
  mqtt_pub(A, Topic, Value, [retain(true), qos(1)]),
  true.


:- dynamic 
   worker_data/2,
   pub_connection/1,
   worker_running/1.

/*


*/



timer_sub :-
  mqtt_connect(P, 'localhost', 1883, 
  	[alias(swi_mqtt_pub), client_id(swi_mqtt_pub_client), keepalive(60), is_async(true), 
     debug_hooks(false), hooks(true), 
     module(user), 
     on_publish(my_on_pub),
     on_disconnect(my_on_dis)
  	]),
  assert(pub_connection(P)),
  mqtt_connect(A, 'localhost', 1883, 
  	[alias(swi_mqtt_timer), client_id(swi_mqtt_timer_client), keepalive(120), is_async(true), 
     debug_hooks(false), hooks(true), 
     module(user), 
     on_message(my_on_msg), on_log(my_on_log),
     on_subscribe(my_on_sub), on_unsubscribe(my_on_uns), 
     on_disconnect(my_on_dis)
  	]),
  create_thread_worker(W),
  assert( worker_running(W) ),
  assert( worker_data(W, A) ),

  mqtt_sub(A, 'timestamp', []),

  true.

/*


*/

simple_shutdown :-
  worker_data(ThreadId, Conn),
  mqtt_unsub(Conn, '#'),
  retractall(worker_running(ThreadId)),
  thread_join(ThreadId, _),
  mqtt_disconnect(Conn),
  retractall(worker_data(ThreadId, Conn)),

  pub_connection(P),
  mqtt_disconnect(P),
  retractall(pub_connection(_)),

  mqtt:c_destroy_engine,
  true.

create_thread_worker(ThreadId) :-
  thread_create((do_mqtt_work_init), ThreadId, [alias(mqtt_worker)]),
  true.

do_mqtt_work_init :-
   thread_self(ThreadId),
   do_mqtt_work(ThreadId).  


do_mqtt_work(ThreadId) :-
   worker_data(ThreadId, _Conn),
   worker_running(ThreadId),
   get_time(TimeRaw),
   % just seconds, no microsecs
   format_time(atom(Time), '%s', TimeRaw),
   %format_time(atom(TimeFmt), '%F %T', TimeRaw), format('% pub time: ~w~n', [TimeFmt]),
   %mqtt_pub(Conn, '/swi/host/timestamp', Time),
   timer_pub('timestamp', Time),
   sleep(10),
   do_mqtt_work(ThreadId),
   true.

/*
:- multifile
  mqtt_hook_on_message/2.

mqtt_hook_on_message(_C, Data) :-
  format('% got msg...'),
  member(payload(T), Data),
  stamp_date_time(T, X, 'UTC'),
  format('% got time: ~w~n', X).
*/


my_on_log(C,D) :- format('% log > ~w - ~w~n', [C,D]).

my_on_msg(_C,D) :- 
  format('% msg > ~w~n', [D]),
  member(topic(timestamp), D),
  member(payload(Time), D),
  format('% msg > got: ~w seconds~n', [Time]),  
  true.

my_on_pub(_C,D) :- 
  format('% pub > ~w~n', [D]),
  true.

my_on_con(C,D) :- format('% con > ~w - ~w~n', [C,D]).
my_on_dis(C,D) :- format('% dis > ~w - ~w~n', [C,D]).

my_on_sub(C,D) :- format('% sub > ~w - ~w~n', [C,D]).
my_on_uns(C,D) :- format('% uns > ~w - ~w~n', [C,D]).
