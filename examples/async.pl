:- use_module(library(mqtt)).

versions:-
  mqtt_version(MqttVerMa, MqttVerMi, MqttVerRe),
  pack_version(PackVerMa, PackVerMi, PackVerRe),
  format('% Pack version: ~w.~w.~w Mosquitto version: ~w.~w.~w~n', [PackVerMa, PackVerMi, PackVerRe, MqttVerMa, MqttVerMi, MqttVerRe]),
  true.


/*




*/
async_pub(Topic, Value) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt1), client_id(swi_mqtt_client1), keepalive(10), is_async(true)]),
  mqtt_pub(A, Topic, Value),
  sleep(5),
  mqtt_disconnect(A).


/*



*/
async_sub(Topic) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt2), client_id(swi_mqtt_client2), keepalive(10), is_async(true)]),
  mqtt_sub(A, Topic, []),
  % mqtt_sub(A, '/#', []),
  true.
  
  
  
  
  
