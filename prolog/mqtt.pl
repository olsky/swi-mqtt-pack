% MQTT pack for SWI-Prolog
% 2016-05-24 - olsky - initial draft 
%

:- module(mqtt, [
]).

:- load_foreign_library(foreign(mqtt)).


:- multifile
  mqtt_hook_connected/2,
  mqtt_hook_disconnected/2.

:- dynamic 
  mqtt_connection/2.

% mqtt_connect(-Connection, +Host) default port 1883
mqtt_connect(Connection, Host) :- 
  mqtt_connect(Connection, Host, 1883),
  true.  

% mqtt_connect(-Connection, +Host, +Port)
mqtt_connect(Connection, Host, Port) :-
  gensym(mqtt_conn_, A),
  gensym(swi_mqtt_client, Cid),
  
  mqtt_connect(Connection, Host, Port, [alias(A), client_id(Cid), keepalive(60)]),
  true.
  
% mqtt_connect(-Connection, +Host, +Port, [Options])
mqtt_connect(Connection, Host, Port, Options) :-
  nonvar(Host),
  nonvar(Port),
  c_mosquitto_connect(Connection, Host, Port, Options),
  (
   member(alias(A), Options)
     -> true
      ;  gensym(mqtt_conn_, A)
  ),
  assert(mqtt_connection(A, Connection)),
  call_mqtt_connected_hook(Connection, [flow(prolog)]),
  true.

% close connection
mqtt_disconnect(Connection) :-
  retract_all(mqtt_connection(_, Connection)),
  c_mosquitto_disconnect(Connection),
  call_mqtt_disconnected_hook(Connection, [flow(prolog)]),
  true.








% hooks:

call_mqtt_connected_hook(Connection, Data) :-
  mqtt_hook_connected(Connection, Data), !.
call_mqtt_connected_hook(_, _).


call_mqtt_disconnected_hook(Connection, Data) :-
  mqtt_hook_disconnected(Connection, Data), !.
call_mqtt_disconnected_hook(_, _).




