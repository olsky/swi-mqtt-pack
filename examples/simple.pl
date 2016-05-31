:- use_module(library(mqtt)).

simple_pub(Topic, Value) :-
  mqtt_connect(A, 'localhost', 1883, [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false)]),
  mqtt_pub(A, Topic, Value),
  mqtt_disconnect(A).

