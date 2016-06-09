% MQTT pack for SWI-Prolog
% 2016-05-24 - olsky - initial draft 
% 2016-05-30 - olsky - working connect and publish
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
mqtt_unsub/2,
mqtt_version/3,
pack_version/3,
mqtt_version/1,
pack_version/1
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


mqtt_loop(C) :-
 c_mqtt_loop(C).

mqtt_version(Ma, Mi, Re) :-
  c_mqtt_version(Ma, Mi, Re).
pack_version(Ma, Mi, Re) :-
 c_pack_version(Ma, Mi, Re).
mqtt_version(Version) :-
  c_mqtt_version(Version).
pack_version(Version) :-
 c_pack_version(Version).


% mqtt_connect(-Connection, +Host) default port 1883
mqtt_connect(Connection, Host) :- 
  mqtt_connect(Connection, Host, 1883),
  true.  

% mqtt_connect(-Connection, +Host, +Port)
mqtt_connect(Connection, Host, Port) :-
  gensym(mqtt_conn_, A),
  gensym(swi_mqtt_client, Cid),
  
  mqtt_connect(Connection, Host, Port, [alias(A), client_id(Cid), keepalive(60), is_async(false)]),
  true.
  
% mqtt_connect(-Connection, +Host, +Port, [Options])
% options are:  
% - alias(A), client_id(Cid), keepalive(10), is_async(false)
% callbacks:
% - module()
% - on_connect() on_disconnect()  on_log() 
% - on_message() on_publish() on_subscribe(), on_unsubscribe()
mqtt_connect(Connection, Host, Port, Options) :-
  nonvar(Host),
  nonvar(Port),
  (
    member(is_async(true), Options)
     -> ignore(c_create_engine)
     ; true
  ),
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
  mqtt_pub(Connection, Topic, Payload, [retain(false), qos(0)]).

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


% unsubscribe from: 
% - topic pattern
mqtt_unsub(Connection, Topic) :-
  c_mqtt_unsub(Connection, Topic),
  true.


% hook dump
mqtt_hook_dump(Event, C, Data) :-
  thread_self(T), 
  format('% hook <~w> ~t <~w> ~t <mqtt:~w> ~t <~w>~n', [T, Event, C, Data]), 
  %sleep(2),
  true.

% hooks:

mqtt_hook_on_connect(C, Data)     :- mqtt_hook_dump(on_connect, C, Data).
mqtt_hook_on_disconnect(C, Data)  :- mqtt_hook_dump(on_disconnect, C, Data).
mqtt_hook_on_message(C, Data)     :- mqtt_hook_dump(on_message, C, Data).
mqtt_hook_on_subscribe(C, Data)   :- mqtt_hook_dump(on_subscribe, C, Data).
mqtt_hook_on_publish(C, Data)     :- mqtt_hook_dump(on_publish, C, Data).
mqtt_hook_on_unsubscribe(C, Data) :- mqtt_hook_dump(on_unsubscribe, C, Data).
mqtt_hook_on_log(C, Data)         :- mqtt_hook_dump(on_log, C, Data).

