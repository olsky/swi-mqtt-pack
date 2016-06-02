:- use_module(library(mqtt)).


versions:-
  mqtt_version(MqttVerMa, MqttVerMi, MqttVerRe),
  pack_version(PackVerMa, PackVerMi, PackVerRe),
  format('% Pack version: ~w.~w.~w Mosquitto version: ~w.~w.~w~n', [PackVerMa, PackVerMi, PackVerRe, MqttVerMa, MqttVerMi, MqttVerRe]),
  true.


/*



*/
timer_pub(Topic, Value) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt_pub), client_id(swi_mqtt_pub_client), keepalive(10), is_async(false)]),
  mqtt_pub(A, Topic, Value),
  mqtt_disconnect(A),
  true.


:- dynamic 
   worker_data/2,
   worker_running/1.

/*


*/



timer_sub :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt_timer), client_id(swi_mqtt_timer_client), keepalive(120), is_async(true)]),
  create_thread_worker(W),
  assert( worker_running(W) ),
  assert( worker_data(W, A) ),

  mqtt_sub(A, '/swi/+/timestamp', []),

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
   get_time(Time),
   %mqtt_pub(Conn, '/swi/host/timestamp', Time),
   timer_pub('/swi/host/timestamp', Time),
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