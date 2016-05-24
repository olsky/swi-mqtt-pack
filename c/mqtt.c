/*
* MQTT foreign library
* uses mosquitto
*
*/

#include <stdio.h>

#include <SWI-Prolog.h>
#include <mosquitto.h>

// TODO add following:
// - define blob to wrap: static struct mosquitto *mosq = NULL;
// - init mosquitto lib (mosquitto_new)
// - cleanup (mosquitto_lib_cleanup)
// - connect/disconnect to/from broker (mosquitto_connect_bind, mosquitto_disconnect)
// - current connections
// - connection discovery predicate
// - publish predicate (mosquitto_publish)
// - subscribe predicate (mosquitto_subscribe)

install_t install_mqtt()
{
}


install_t uninstall_mqtt()
{
}
