:- use_module(library(mqtt)).


versions:-
  mqtt_version(MqttVerMa, MqttVerMi, MqttVerRe),
  pack_version(PackVerMa, PackVerMi, PackVerRe),
  format('% Pack version: ~w.~w.~w Mosquitto version: ~w.~w.~w~n', [PackVerMa, PackVerMi, PackVerRe, MqttVerMa, MqttVerMi, MqttVerRe]),
  true.


/*


3 ?- simple_pub('topicone', test).
% hook > on_log         > connection: <swi_mqtt>(0x88a460-0x8a45b0) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x88a460-0x8a45b0) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x88a460-0x8a45b0) data: [level(16),log(Client swi_mqtt_client sending PUBLISH (d0, q0, r0, m1, 'topicone', ... (4 bytes)))]
% hook > on_publish     > connection: <swi_mqtt>(0x88a460-0x8a45b0) data: [message_id(1)]
% hook > on_log         > connection: <swi_mqtt>(0x88a460-0x8a45b0) data: [level(16),log(Client swi_mqtt_client sending DISCONNECT)]
% hook > on_connect     > connection: <swi_mqtt>(0x88a460-0x8a45b0) data: [reason(0)]
true.


*/
simple_pub(Topic, Value) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_pub(A, Topic, Value),
  mqtt_disconnect(A),
  true.


:- dynamic 
   worker_data/2,
   worker_running/1.

/*


4 ?- simple_sub('topicino').
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client sending SUBSCRIBE (Mid: 1, Topic: topicino, QoS: 0))]
true.

% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client received CONNACK)]
% hook > on_connect     > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [result(0)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client received SUBACK)]
% hook > on_subscribe   > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [message_id(1)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client received PUBLISH (d0, q0, r0, m0, 'topicino', ... (10 bytes)))]
% hook > on_message     > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [message_id(0),topic(topicino),payload(test value),payload_len(10),qos(0),retain(0)]

*/

simple_sub(Topic) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_sub(A, Topic, []),
  create_thread_worker(W),
  assert( worker_running(W) ),
  assert( worker_data(W, A) ),

  true.

/*


6 ?- simple_shutdown.
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client sending UNSUBSCRIBE (Mid: 2, Topic: #))]
% hook > on_log         > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [level(16),log(Client swi_mqtt_client sending DISCONNECT)]
% hook > on_connect     > connection: <swi_mqtt>(0x8a5c20-0x8a5eb0) data: [reason(0)]
true.


*/

simple_shutdown :-
  worker_data(ThreadId, Conn),
  mqtt_unsub(Conn, '#'),
  mqtt_loop(Conn),
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
