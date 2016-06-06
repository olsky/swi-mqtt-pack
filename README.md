# swi-mqtt-pack

This pack uses mosquitto library in the foreign code to add MQTT functionality to SWI-Prolog.

Supported features:

- async and sync connections
- mosquitto connection is stored in the PL_blob > this offers multiple mqtt connections
- connect/pub/sub/disconnect predicates in prolog
- custom callback per connection supported (set in options when creating connection)
- multithreaded support: an additional engine will be created and attached to mosquito thread to perform callbacks

