:- use_module(library(mqtt)).

versions:-
  mqtt_version(MqttVerMa, MqttVerMi, MqttVerRe),
  pack_version(PackVerMa, PackVerMi, PackVerRe),
  format('% Pack version: ~w.~w.~w Mosquitto version: ~w.~w.~w~n', [PackVerMa, PackVerMi, PackVerRe, MqttVerMa, MqttVerMi, MqttVerRe]),
  true.


/*


*/
simple_pub(Topic, Value) :-

  mqtt_connect(A, 'localhost', 1883, 
    [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false), 
     debug_hooks(false), hooks(true), 
     module(user), 
     on_log(my_on_log), on_message(my_on_msg), on_publish(my_on_pub), 
     on_subscribe(my_on_sub), on_unsubscribe(my_on_uns), 
     on_connect(my_on_con), on_disconnect(my_on_dis)
    ]),
  mqtt_pub(A, Topic, Value),
  mqtt_disconnect(A).


/*


*/
simple_sub(Topic) :-
  mqtt_connect(A, 'localhost', 1883, 
    [alias(swi_mqtt), client_id(swi_mqtt_client), keepalive(120), is_async(false),
     debug_hooks(false), hooks(true), 
     module(user), 
     on_log(my_on_log), on_message(my_on_msg), on_publish(my_on_pub), 
     on_subscribe(my_on_sub), on_unsubscribe(my_on_uns), 
     on_connect(my_on_con), on_disconnect(my_on_dis)    
    ]),
  mqtt_sub(A, Topic, []),
  mqtt_loop(A),sleep(1),
  mqtt_loop(A),sleep(1),
  mqtt_loop(A),
  mqtt_unsub(A, Topic),
  mqtt_disconnect(A).


my_on_log(C,D) :- format('% log > ~w - ~w~n', [C,D]).

my_on_msg(C,D) :- format('% msg > ~w - ~w~n', [C,D]).
my_on_pub(C,D) :- format('% pub > ~w - ~w~n', [C,D]).

my_on_con(C,D) :- format('% con > ~w - ~w~n', [C,D]).
my_on_dis(C,D) :- format('% dis > ~w - ~w~n', [C,D]).

my_on_sub(C,D) :- format('% sub > ~w - ~w~n', [C,D]).
my_on_uns(C,D) :- format('% uns > ~w - ~w~n', [C,D]).
