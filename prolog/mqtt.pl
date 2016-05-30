% MQTT pack for SWI-Prolog
% 2016-05-24 - olsky - initial draft 
% 2016-05-30 - olsky - working connect and publish
% 2016-05-30 - olsky - working subscribe and loop
%

:- module(mqtt, [
mqtt_connect/2,
mqtt_connect/3,
mqtt_connect/4,
mqtt_disconnect/1,
mqtt_loop/1,
mqtt_pub/4,
mqtt_pub/3,
mqtt_sub/3,
mqtt_sub/2,
mqtt_version/3,
pack_version/3
]).

:- load_foreign_library(foreign(mqtt)).


:- multifile
  mqtt_hook_on_connect/2,
  mqtt_hook_on_disconnect/2,
  mqtt_hook_on_message/2,
  mqtt_hook_on_subscribe/2,
  mqtt_hook_on_publish/2,
  mqtt_hook_on_unsubscribe/2,
  mqtt_hook_on_log/2.
  
:- dynamic 
  mqtt_connection/2.

% for sync connections, need to call it frequently
mqtt_loop(C) :-
 c_mqtt_loop(C).

mqtt_version(Ma, Mi, Re) :-
  c_mqtt_version(Ma, Mi, Re).
pack_version(Ma, Mi, Re) :-
 c_pack_version(Ma, Mi, Re).


% mqtt_connect(-Connection, +Host) default port 1883
mqtt_connect(Connection, Host) :- 
  mqtt_connect(Connection, Host, 1883),
  true.  

% mqtt_connect(-Connection, +Host, +Port)
mqtt_connect(Connection, Host, Port) :-
  gensym(mqtt_conn_, A),
  gensym(swi_mqtt_client, Cid),
  
  mqtt_connect(Connection, Host, Port, [alias(A), client_id(Cid), keepalive(10), is_async(false)]),
  true.
  
% mqtt_connect(-Connection, +Host, +Port, [Options])
mqtt_connect(Connection, Host, Port, Options) :-
  nonvar(Host),
  nonvar(Port),
  c_mqtt_connect(Connection, Host, Port, Options),
  (
   member(alias(A), Options)
     -> true
      ;  gensym(mqtt_conn_, A)
  ),
  assert(mqtt_connection(A, Connection)),
  % needed? call_mqtt_connected_hook(Connection, [flow(prolog)]),
  true.




% close connection
mqtt_disconnect(Connection) :-
  (c_mqtt_disconnect(Connection)
    -> retractall(mqtt_connection(_, Connection))
    ; fail
  ),
  % needed? call_mqtt_disconnected_hook(Connection, [flow(prolog)]),
  true.

mqtt_pub(Connection, Topic, Payload) :-
  mqtt_pub(Connection, Topic, Payload, [retain(0), qos(0)]).

% publish to mqtt
mqtt_pub(Connection, Topic, Payload, Options) :-
  c_mqtt_pub(Connection, Topic, Payload, Options),
  true.


% subscribe with: 
% - topic pattern
% - qos
mqtt_sub(Connection, Topic, Options) :-
  c_mqtt_sub(Connection, Topic, Options),
  true.
mqtt_sub(Connection, Topic) :-
  mqtt_sub(Connection, Topic, []).


% hooks:

mqtt_hook_on_connect(Connection, Data)     :- format('% hook > on_connect     > connection: ~w data: ~w~n', [Connection, Data]).
mqtt_hook_on_disconnect(Connection, Data)  :- format('% hook > on_disconnect  > connection: ~w data: ~w~n', [Connection, Data]).
mqtt_hook_on_message(Connection, Data)     :- format('% hook > on_message     > connection: ~w data: ~w~n', [Connection, Data]).
mqtt_hook_on_subscribe(Connection, Data)   :- format('% hook > on_subscribe   > connection: ~w data: ~w~n', [Connection, Data]).
mqtt_hook_on_publish(Connection, Data)     :- format('% hook > on_publish     > connection: ~w data: ~w~n', [Connection, Data]).
mqtt_hook_on_unsubscribe(Connection, Data) :- format('% hook > on_unsubscribe > connection: ~w data: ~w~n', [Connection, Data]).
mqtt_hook_on_log(Connection, Data)         :- format('% hook > on_log         > connection: ~w data: ~w~n', [Connection, Data]).

