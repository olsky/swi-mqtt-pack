# swi-mqtt-pack


an example test:


?- use_module(library(mqtt)), module(mqtt).
 
true.

mqtt: 2 ?- mqtt_connect(A, localhost), mqtt_pub(A, topicone, 00).

% hook > on_log         > connection: <swi_mqtt>(0x91cde0-0x91c070) data: [level(16),log(Client swi_mqtt_client1 sending CONNECT)]

% hook > on_log         > connection: <swi_mqtt>(0x91cde0-0x91c070) data: [level(16),log(Client swi_mqtt_client1 sending CONNECT)]

% hook > on_log         > connection: <swi_mqtt>(0x91cde0-0x91c070) data: [level(16),log(Client swi_mqtt_client1 sending PUBLISH (d0, q0, r0, m1, 'topicone', ... (1 bytes)))]

% hook > on_publish     > connection: <swi_mqtt>(0x91cde0-0x91c070) data: [message_id(1)]

A = <swi_mqtt>(0x91cde0-0x91c070).


mqtt: 2 ?- pack_version(Major, Minor, Rev).
Major = Minor, Minor = 0,
Rev = 2.

mqtt: 3 ?- mqtt_version(Major, Minor, Rev).
Major = 1,
Minor = 4,
Rev = 8.

