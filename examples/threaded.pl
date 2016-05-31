:- use_module(library(mqtt)).

simple_pub(Topic, Value) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_pub(A, Topic, Value),
  mqtt_disconnect(A),
  true.


:- dynamic 
   worker_data/2,
   worker_running/1.



simple_sub(Topic) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_sub(A, Topic, []),
  create_thread_worker(W),
  assert( worker_running(W) ),
  assert( worker_data(W, A) ),

  true.

simple_shutdown :-
  worker_data(ThreadId, Conn),
  retractall(worker_running(ThreadId)),
  thread_join(ThreadId, _),
  mqtt_disconnect(Conn),
  retractall(worker_data(ThreadId, Conn)),
  true.

create_thread_worker(ThreadId) :-
  thread_create((do_mqtt_work_init), ThreadId, [alias(mqtt_worker)]),
  true.

do_mqtt_work_init :-
   thread_self(ThreadId),
   do_mqtt_work(ThreadId).  


do_mqtt_work(ThreadId) :-
   worker_data(ThreadId, Conn),
   worker_running(ThreadId),
   mqtt_loop(Conn),
   sleep(5),
   do_mqtt_work(ThreadId),
   true.
