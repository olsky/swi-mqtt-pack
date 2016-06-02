:- use_module(library(mqtt)).

versions:-
  mqtt_version(MqttVerMa, MqttVerMi, MqttVerRe),
  pack_version(PackVerMa, PackVerMi, PackVerRe),
  format('% Pack version: ~w.~w.~w Mosquitto version: ~w.~w.~w~n', [PackVerMa, PackVerMi, PackVerRe, MqttVerMa, MqttVerMi, MqttVerRe]),
  true.


/*


3 ?- simple_pub('topicone', test).
% hook > on_log         > connection: <swi_mqtt>(0x8a5980-0x8a5ae0) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5980-0x8a5ae0) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5980-0x8a5ae0) data: [level(16),log(Client swi_mqtt_client sending PUBLISH (d0, q0, r0, m1, 'topicone', ... (4 bytes)))]
% hook > on_publish     > connection: <swi_mqtt>(0x8a5980-0x8a5ae0) data: [message_id(1)]
% hook > on_log         > connection: <swi_mqtt>(0x8a5980-0x8a5ae0) data: [level(16),log(Client swi_mqtt_client sending DISCONNECT)]
% hook > on_disconnect  > connection: <swi_mqtt>(0x8a5980-0x8a5ae0) data: [reason(0)]
true.



*/
simple_pub(Topic, Value) :-

  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_pub(A, Topic, Value),
  mqtt_disconnect(A).


/*



2 ?- simple_sub('topicino').
% hook > on_log         > connection: <swi_mqtt>(0x889f20-0x889f50) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x889f20-0x889f50) data: [level(16),log(Client swi_mqtt_client sending CONNECT)]
% hook > on_log         > connection: <swi_mqtt>(0x889f20-0x889f50) data: [level(16),log(Client swi_mqtt_client sending SUBSCRIBE (Mid: 1, Topic: topicino, QoS: 0))]
% hook > on_log         > connection: <swi_mqtt>(0x889f20-0x889f50) data: [level(16),log(Client swi_mqtt_client sending UNSUBSCRIBE (Mid: 2, Topic: topicino))]
% hook > on_log         > connection: <swi_mqtt>(0x889f20-0x889f50) data: [level(16),log(Client swi_mqtt_client sending DISCONNECT)]
% hook > on_disconnect  > connection: <swi_mqtt>(0x889f20-0x889f50) data: [reason(0)]
true.




*/
simple_sub(Topic) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_sub(A, Topic, []),
  mqtt_loop(A),
  
  mqtt_loop(A),
  mqtt_unsub(A, Topic),
  mqtt_disconnect(A).
