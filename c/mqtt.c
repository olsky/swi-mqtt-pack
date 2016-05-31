/*
* MQTT foreign library
* uses mosquitto
*
*/

#include <stdio.h>
#include <stdarg.h>
#include <signal.h>

#include <string.h>
#include <stdlib.h>

#include <SWI-Stream.h>
#include <SWI-Prolog.h>

#include <mosquitto.h>

#define O_PLMT 1


#define DEBUG_FOREIGN 1

#ifdef DEBUG_FOREIGN
#define _LOG(...)    printf(__VA_ARGS__);fflush(stdout);
#else
#define _LOG(...) /*no op*/
#endif

#define PACK_MAJOR 0
#define PACK_MINOR 0
#define PACK_REVISION 2
#define PACK_VERSION_NUMBER (PACK_MAJOR*1000000+PACK_MINOR*1000+PACK_REVISION)


#ifdef O_PLMT_MF
#define LOCK(mf)   pthread_mutex_lock(&(mf)->mutex)
#define UNLOCK(mf) pthread_mutex_unlock(&(mf)->mutex)
#else
#define LOCK(mf)
#define UNLOCK(mf)
#endif


#define MKATOM(n) ATOM_ ## n = PL_new_atom(#n);

static atom_t ATOM_is_async;
static atom_t ATOM_client_id;
static atom_t ATOM_keepalive;
static atom_t ATOM_use_callbacks;

static atom_t ATOM_message_id;
static atom_t ATOM_topic;
static atom_t ATOM_payload;
static atom_t ATOM_payload_len;
static atom_t ATOM_qos;
static atom_t ATOM_retain;

static atom_t ATOM_bind_address;

static atom_t ATOM_protocol_version; // V31 or V311
static atom_t ATOM_v31;
static atom_t ATOM_v311;

//static atom_t ATOM_flow;
//static atom_t ATOM_foreign;
//static atom_t ATOM_prolog;

static atom_t ATOM_level;
static atom_t ATOM_log;

static atom_t ATOM_payload_type;
static atom_t ATOM_payload_type_raw;
static atom_t ATOM_payload_type_char;

static atom_t ATOM_result;
static atom_t ATOM_reason;

// these are options going with hook to prolog
static functor_t FUNCTOR_topic1;
static functor_t FUNCTOR_payload1;
static functor_t FUNCTOR_payload_len1;
static functor_t FUNCTOR_qos1;
static functor_t FUNCTOR_retain1;
static functor_t FUNCTOR_message_id1;

//static functor_t FUNCTOR_flow1;

static functor_t FUNCTOR_level1;
static functor_t FUNCTOR_log1;

static functor_t FUNCTOR_result1;
static functor_t FUNCTOR_reason1;

// hook predicates to call from here:

static predicate_t PRED_on_connect2;
static predicate_t PRED_on_disconnect2;
static predicate_t PRED_on_log2;
static predicate_t PRED_on_message2;
static predicate_t PRED_on_publish2;
static predicate_t PRED_on_subscribe2;
static predicate_t PRED_on_unsubscribe2;

/*
  mqtt_hook_on_connect/2,
  mqtt_hook_on_disconnect/2,
  mqtt_hook_on_message/2,
  mqtt_hook_on_subscribe/2,
  mqtt_hook_on_publish/2,
  mqtt_hook_on_unsubscribe/2,
  mqtt_hook_on_log/2.
*/




typedef struct
{
  struct mosquitto    *mosq;                   /* mosquito structure */
  bool          is_async;               /* if true then use mosquitto_connect_async */
  atom_t        symbol;                 /* <swi_mqtt>(%p) */
  bool          is_async_loop_started;  /* set to true after mosquitto_loop_start call */
  int           keepalive;
  int           loop_max_packets;
} swi_mqtt;
  

static int unify_swi_mqtt(term_t handle, swi_mqtt *m);
  
void handle_signal(int s)
{
  /* dummy signal handler */
}

static int
add_int_option(term_t list, functor_t f, int value)
{
  // type is of: PL_INTEGER
  term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();

  while(PL_get_list(tail, head, tail))
  { if ( PL_unify_functor(head, f) )
    { term_t a = PL_new_term_ref();
      return (PL_get_arg(1, head, a) && PL_unify_integer(a, value));
    }
  }

  if ( PL_unify_list(tail, head, tail) ){
    return PL_unify_term(head, PL_FUNCTOR, f, PL_INTEGER, value);
  }
  return FALSE;
}
static int
add_char_option(term_t list, functor_t f, const char *value)
{
  // type is of: PL_CHARS
  term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();

  while(PL_get_list(tail, head, tail))
  { if ( PL_unify_functor(head, f) )
    { term_t a = PL_new_term_ref();
      term_t tmp = PL_new_term_ref();
      PL_put_atom_chars(tmp, value);

      return (PL_get_arg(1, head, a) && PL_unify(a, tmp));
    }
  }

  if ( PL_unify_list(tail, head, tail) )
  {
    return PL_unify_term(head, PL_FUNCTOR, f, PL_CHARS, value);
  }
  return FALSE;
}

static int
close_list(term_t list)
{ term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();

  while(PL_get_list(tail, head, tail))
    ;

  return PL_unify_nil(tail);
}


/*
static int
get_time_option(term_t list, functor_t f, time_t def, time_t *tme)
{ term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();

  while(PL_get_list(tail, head, tail))
  { if ( PL_is_functor(head, f) )
    { term_t a = PL_new_term_ref();
      double f;

      _PL_get_arg(1, head, a);
      if ( !PL_get_float(a, &f) )
      { atom_t now;

	if ( PL_get_atom(a, &now) && now == ATOM_now )
	{ time(tme);
	  return TRUE;
	} else
	  return pl_error(NULL, 0, NULL, ERR_TYPE, a, "time");
      }
      *tme = (long)f;
      return TRUE;
    }
  }

  *tme = def;
  return TRUE;
}
*/







static int
destroy_mqtt(swi_mqtt *m)
{ 
  _LOG("--- (f-c) destroy_mqtt\n");

  mosquitto_destroy(m->mosq);
  free(m);
  return TRUE;
}



		 /*******************************
		 *	      CALLBACKS		*
		 *******************************/

void on_disconnect_callback(struct mosquitto *mosq, void *obj, int reason)
{
swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_disconnect_callback > mosq: %p obj: %p-%p\n", mosq, obj, m->mosq);

  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);

  add_int_option( t1, FUNCTOR_reason1,  reason);
  close_list(t1);

  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_connect2, t0); 
  PL_discard_foreign_frame(fid);
}


void on_connect_callback(struct mosquitto *mosq, void *obj, int result)
{
  swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_connect_callback > mosq: %p obj: %p-%p\n", mosq, obj, m->mosq);

  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);
  add_int_option( t1, FUNCTOR_result1,  result);
  close_list(t1);

  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_connect2, t0); 
  PL_discard_foreign_frame(fid);
}

void on_message_callback(struct mosquitto *mosq, void *obj, const struct mosquitto_message *message)
{
  swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_message_callback > mosq: %p obj: %p-%p\n", mosq, obj, m->mosq);
 
  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);

  add_int_option( t1, FUNCTOR_message_id1,  message->mid);
  add_char_option(t1, FUNCTOR_topic1,       message->topic);
  add_char_option(t1, FUNCTOR_payload1,     message->payload);
  add_int_option( t1, FUNCTOR_payload_len1, message->payloadlen);
  add_int_option( t1, FUNCTOR_qos1,         message->qos);
  add_int_option( t1, FUNCTOR_retain1,      message->retain);
  close_list(t1);


  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_message2, t0);  
  PL_discard_foreign_frame(fid);
}

void on_publish_callback(struct mosquitto *mosq, void *obj, int mid)
{
  swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_publish_callback > mosq: %p obj: %p-%p\n", mosq, obj, m->mosq);

  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);

  add_int_option( t1, FUNCTOR_message_id1,  mid);
  close_list(t1);


  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_publish2, t0); 
  PL_discard_foreign_frame(fid);
}


void on_log_callback(struct mosquitto *mosq, void *obj, int level, const char *str)
{
  swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_log_callback > mosq: %p obj: %p-%p log str: %s\n", mosq, obj, m->mosq, str);

  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);

  add_int_option( t1, FUNCTOR_level1,  level);
  add_char_option(t1, FUNCTOR_log1,    str);
  close_list(t1);


  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_log2, t0); 
  PL_discard_foreign_frame(fid);
}


void on_subscribe_callback(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos)
{
  swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_subscribe_callback > mosq: %p obj: %p-%p mid: %d\n", mosq, obj, m->mosq, mid);

  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);

  add_int_option( t1, FUNCTOR_message_id1,  mid);
  //add_int_option(t1, FUNCTOR_log1,    qos_count);
  close_list(t1);


  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_subscribe2, t0); 
  PL_discard_foreign_frame(fid);
  
 /*  obj -         the user data provided in <mosquitto_new>
 *  mid -         the message id of the subscribe message.
 *  qos_count -   the number of granted subscriptions (size of granted_qos).
 *  granted_qos - an array of integers indicating the granted QoS for each of
 *                the subscriptions.
 */

}



void on_unsubscribe_callback(struct mosquitto *mosq, void *obj, int mid)
{
  swi_mqtt *m = (swi_mqtt *)obj;

  _LOG("--- (f-c) on_unsubscribe_callback > mosq: %p obj: %p-%p mid: %d\n", mosq, obj, m->mosq, mid);

  fid_t fid = PL_open_foreign_frame();
  term_t t0 = PL_new_term_refs(2);
  term_t t1 = t0 + 1;
  unify_swi_mqtt(t0, m);

  add_int_option( t1, FUNCTOR_message_id1,  mid);
  close_list(t1);


  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_unsubscribe2, t0); 
  PL_discard_foreign_frame(fid);
  }






		 /*******************************
		 *	      SYMBOL		*
		 *******************************/

static void
acquire_mqtt_symbol(atom_t symbol)
{ 
  _LOG("--- (f-c) acquire_mqtt_symbol\n");

  swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  m->symbol = symbol;
}

static int
release_mqtt_symbol(atom_t symbol)
{ 
  _LOG("--- (f-c) release_mqtt_symbol\n");

  swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  destroy_mqtt(m);
  return TRUE;
}

static int
compare_mqtt_symbols(atom_t a, atom_t b)
{ 
  _LOG("--- (f-c) compare_mqtt_symbols\n");

  swi_mqtt *ma = PL_blob_data(a, NULL, NULL);
  swi_mqtt *mb = PL_blob_data(b, NULL, NULL);
  return ( ma > mb ?  1 :
	   ma < mb ? -1 : 0
	 );
}

static int
write_mqtt_symbol(IOSTREAM *s, atom_t symbol, int flags)
{ 
  _LOG("--- (f-c) write_mqtt_symbol\n");

  swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  Sfprintf(s, "<swi_mqtt>(%p-%p)", m, m->mosq);
  return TRUE;
}

static PL_blob_t mqtt_blob =
{ PL_BLOB_MAGIC,
  PL_BLOB_NOCOPY,
  "mqtt_connection",
  release_mqtt_symbol,
  compare_mqtt_symbols,
    write_mqtt_symbol,
  acquire_mqtt_symbol
};

static int
unify_swi_mqtt(term_t handle, swi_mqtt *m)
{
  _LOG("--- (f-c) unify_swi_mqtt\n");

  if ( PL_unify_blob(handle, m, sizeof(*m), &mqtt_blob) )
    return TRUE;
  if ( !PL_is_variable(handle) )
    return PL_uninstantiation_error(handle);
  return FALSE;					/* (resource) error */
}

static int
get_swi_mqtt(term_t handle, swi_mqtt **mp)
{ PL_blob_t *type;

  _LOG("--- (f-c) get_swi_mqtt\n");

  void *data;
  if ( PL_get_blob(handle, &data, NULL, &type) && type == &mqtt_blob)
  { swi_mqtt *m = data;
    // assert(mf->magic == MEMFILE_MAGIC);
    LOCK(m);
    if ( m->symbol )
    { *mp = m;
      return TRUE;
    }
    PL_permission_error("access", "freed_swi_mqtt", handle);
    return FALSE;
  }
  return PL_type_error("swi_mqtt", handle);
}


static void
release_swi_mqtt(swi_mqtt *m)
{ 
  _LOG("--- (f-c) release_swi_mqtt\n");
  UNLOCK(m);
}


/*
static void
empty_swi_mqtt(swi_mqtt *m)
{ if ( m->data )
    free(m->data);

  // init with default values
  m->symbol = 0;
  m->is_async              = TRUE;
  m->is_async_loop_started = FALSE;
  m->keepalive             = 60;
  m->loop_max_packets      = 10;
}
*/



     /*******************************
     *        FOREIGN PREDICATES    *
     *******************************/


static foreign_t
c_free_swi_mqtt(term_t handle)
{ swi_mqtt *m;
  _LOG("--- (f-c) c_free_swi_mqtt\n");

  if ( get_swi_mqtt(handle, &m) )
  {
    m->symbol = 0;
    mosquitto_destroy(m->mosq);
    release_swi_mqtt(m);
    return TRUE;
  }
  return FALSE;
}

static foreign_t 
c_pack_version1(term_t ver)
{
  if (PL_unify_integer(ver, PACK_VERSION_NUMBER))
  {
    return TRUE; 
  }
  return FALSE;
}


static foreign_t 
c_pack_version3(term_t maj, term_t min, term_t rev)
{
  if ( 
      PL_unify_integer(maj, PACK_MAJOR)
      &&
      PL_unify_integer(min, PACK_MINOR)
      &&
      PL_unify_integer(rev, PACK_REVISION)
     )
  {
    return TRUE; 
  }
  return FALSE;
}



static foreign_t 
c_mqtt_version1(term_t ver)
{
  if ( PL_unify_integer(ver, LIBMOSQUITTO_VERSION_NUMBER))
  {
    return TRUE; 
  }
  return FALSE;
}
static foreign_t 
c_mqtt_version3(term_t maj, term_t min, term_t rev)
{
  if ( 
      PL_unify_integer(maj, LIBMOSQUITTO_MAJOR)
      &&
      PL_unify_integer(min, LIBMOSQUITTO_MINOR)
      &&
      PL_unify_integer(rev, LIBMOSQUITTO_REVISION)
     )
  {
    return TRUE; 
  }
  return FALSE;
}



static foreign_t 
c_mqtt_disconnect(term_t conn)
{
  swi_mqtt *m;
  _LOG("--- (f-c) c_mqtt_disconnect\n");

  if (!get_swi_mqtt(conn, &m)) return FALSE;

  _LOG("--- (f-c) c_mqtt_disconnect > have connection %p\n", m->mosq);

  if (mosquitto_disconnect(m->mosq) == MOSQ_ERR_SUCCESS)
  {
      return TRUE;
  }
  

  return FALSE;
}




// in options: [type(bin|char|double|int), qos(0|1|2), retain(true|false)]
static foreign_t 
c_mqtt_pub(term_t conn, term_t topic, term_t payload, term_t options)
{
  int result = FALSE;

  swi_mqtt *m;
  int mid;
  char buf[81];
  char* mqtt_topic   = NULL;
  char* mqtt_payload = NULL;
  char* payload_type = NULL;
  int qos = 0;

  int mosq_rc;
  struct mosquitto *mosq;
 
  /* logging */
  _LOG("--- (f-c) c_mqtt_pub\n");

  if (!get_swi_mqtt(conn, &m))
  {
    result = FALSE;
    goto CLEANUP;
  }

  mosq = m->mosq;
  _LOG("--- (f-c) c_mqtt_pub > have connection %p (mosq: %p)\n", m->mosq, mosq);

  mosq_rc = mosquitto_reconnect(mosq);
  if (mosq_rc == MOSQ_ERR_SUCCESS)
  {
    _LOG("--- (f-c) c_mqtt_pub > mosquitto_reconnect-ed\n");
  } else {
    _LOG("--- (f-c) c_mqtt_pub > mosquitto_reconnect failed: %d\n", mosq_rc);
  }


  if (!PL_get_chars(topic, &mqtt_topic, CVT_WRITE | BUF_MALLOC)) { 
    result = FALSE;
    goto CLEANUP;
  }
  _LOG("--- (f-c) c_mqtt_pub > topic is: %s\n", mqtt_topic);
/*
  if ( options )
  { 
      term_t tail = PL_copy_term_ref(options);
      term_t head = PL_new_term_ref();

      while(PL_get_list(tail, head, tail))
      { 
        size_t arity;
        atom_t name;

        if ( PL_get_name_arity(head, &name, &arity) && arity == 1 )
        { 
            term_t arg = PL_new_term_ref();

            _PL_get_arg(1, head, arg);
    
                   if ( name == ATOM_payload_type ) { if (!PL_get_chars(  arg, &payload_type, CVT_WRITE|BUF_MALLOC) ) { result = FALSE;goto CLEANUP; }
            } else if ( name == ATOM_qos          ) { if (!PL_get_integer(arg, &qos) ) { result = FALSE;goto CLEANUP; }
            }
    
        } else { 
            result = pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, head, "option");
            goto CLEANUP;
        }
      }
      // unify with NIL --> end of list
      if ( !PL_get_nil(tail) )
      { 
        result = pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, tail, "list");
        goto CLEANUP;
      }
  }
*/
  if (!PL_get_chars(payload, &mqtt_payload, CVT_WRITE | BUF_MALLOC)) { 
    result = FALSE;
    goto CLEANUP;
  }

  _LOG("--- (f-c) c_mqtt_pub > payload is: %s\n", mqtt_payload);

  memset(buf, 0, 81*sizeof(char));
  snprintf(buf, 80, "%s", mqtt_payload);

  _LOG("--- (f-c) c_mqtt_pub > publish...\n");
  mosq_rc = mosquitto_publish(m->mosq, &mid, mqtt_topic, strlen(buf), buf, qos, 0);
  if (mosq_rc == MOSQ_ERR_SUCCESS)
  {
    _LOG("--- (f-c) c_mqtt_pub > publish done\n");
    result = TRUE;
    goto CLEANUP;
  } else {
    _LOG("--- (f-c) c_mqtt_pub > publish failed rc: %d\n", mosq_rc);
  }


CLEANUP:
  PL_free(mqtt_topic);
  PL_free(payload_type);

  return result;
}


static foreign_t 
c_mqtt_connect(term_t conn, term_t host, term_t port, term_t options)
{
  int result = FALSE;

  struct mosquitto *mosq;
  int mosq_rc = 1;

  char* mqtt_host = NULL;
  int mqtt_port = 1883;

  char* client_id;
  int keepalive = 60;
  int is_async = FALSE;
  
  _LOG("--- (f-c) c_mqtt_connect\n");


  if ( !PL_get_integer_ex(port, &mqtt_port))
  {
    result = FALSE;
    goto CLEANUP;
  }
  

  if (!PL_get_chars(host, &mqtt_host, CVT_WRITE | BUF_MALLOC)) { 
    result = FALSE;
    goto CLEANUP;
  }
  _LOG("--- (f-c) c_mqtt_connect > mqtt host: %s port: %d\n", mqtt_host, mqtt_port);

    
  if ( options )
  { 
      _LOG("--- (f-c) c_mqtt_connect > loop_options...\n");

      term_t tail = PL_copy_term_ref(options);
      term_t head = PL_new_term_ref();

      while(PL_get_list(tail, head, tail))
      { 
        int arity;
	      atom_t name;

        if ( PL_get_name_arity(head, &name, &arity) && arity == 1 )
        { 
            term_t arg = PL_new_term_ref();

            _PL_get_arg(1, head, arg);
	  
  	        if        ( name == ATOM_client_id ) { if (!PL_get_chars(  arg, &client_id, CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}
  	        } else if ( name == ATOM_is_async )  { if (!PL_get_bool(   arg, &is_async)  ) { result = FALSE;goto CLEANUP;}
  	        } else if ( name == ATOM_keepalive ) { if (!PL_get_integer(arg, &keepalive) ) { result = FALSE; goto CLEANUP;}
            }
	  
        } else { 
            result = FALSE; //pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, head, "option");
            goto CLEANUP;
        }
      }
      // unify with NIL --> end of list
      if ( !PL_get_nil(tail) )
      { 
        result = FALSE; //pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, tail, "list");
        goto CLEANUP;
      }
  }

  mosquitto_lib_init();

  _LOG("--- (f-c) c_mqtt_connect > init mems...\n");
  // allocate mem for struct
  swi_mqtt *m = calloc(1, sizeof(*m));

  if ( !m ) {
    result = PL_resource_error("memory");
    goto CLEANUP;
  }
  
  // pass swi_mqtt as user object (will be available in all callbacks)
  mosq = mosquitto_new(client_id, true, m);
  if (mosq)
  {
    _LOG("--- (f-c) c_mqtt_connect > new mqtt: %p\n", mosq);
    m->mosq	= mosq;
    _LOG("--- (f-c) c_mqtt_connect > m->mosq: %p\n", m->mosq);
    m->is_async	= is_async;
    m->is_async_loop_started = false;

    m->keepalive = keepalive;

    // if ATOM_use_callbacks set -->
    // set all callbacks
    mosquitto_log_callback_set(m->mosq, on_log_callback);

    mosquitto_connect_callback_set(m->mosq, on_connect_callback);
    mosquitto_disconnect_callback_set(m->mosq, on_disconnect_callback);

    mosquitto_message_callback_set(m->mosq, on_message_callback);
    mosquitto_publish_callback_set(m->mosq, on_publish_callback);

    mosquitto_subscribe_callback_set(m->mosq, on_subscribe_callback);
    mosquitto_unsubscribe_callback_set(m->mosq, on_unsubscribe_callback);


    
    if (is_async) {
      _LOG("--- (f-c) c_mqtt_connect > connect async...\n");
      // with bind: rc = mosquitto_connect_bind_async(m->mosq, mqtt_host, mqtt_port, keepalive, NULL);
      mosq_rc = mosquitto_connect_async(m->mosq, mqtt_host, mqtt_port, keepalive);
      if (mosq_rc > 0)
      {
        _LOG("--- (f-c) c_mqtt_connect > mosquitto_connect_async failed: %d\n", mosq_rc);
        result = PL_resource_error("mqtt_connect_failed");
        goto CLEANUP;
      }
      _LOG("--- (f-c) c_mqtt_connect > connect async done, starting loop...\n");
      mosq_rc = mosquitto_loop_start(m->mosq);
      if (mosq_rc == MOSQ_ERR_SUCCESS)
      {
        m->is_async_loop_started = true;
        _LOG("--- (f-c) c_mqtt_connect > mosquitto_loop_start done\n");
      } else {
        _LOG("--- (f-c) c_mqtt_connect > mosquitto_loop_start failed: %d\n", mosq_rc);
        result = PL_resource_error("mqtt_loop_start_failed");
        goto CLEANUP;        
      }
    } else {
      mosq_rc = mosquitto_threaded_set(m->mosq, is_async);
      if (mosq_rc == MOSQ_ERR_SUCCESS)
      {
        _LOG("--- (f-c) c_mqtt_connect > thread set done\n");
      } else {
          _LOG("--- (f-c) c_mqtt_connect > thread set failed: %d\n", mosq_rc);
          result = PL_resource_error("mqtt_loop_not_supported_or_invalid");
          goto CLEANUP;              
      }


      _LOG("--- (f-c) c_mqtt_connect > connect sync...\n");
      // call it for sync: mosquitto_threaded_set(m->mosq, true);
      // with bind: rc = mosquitto_connect_bind(m->mosq, mqtt_host, mqtt_port, keepalive, NULL);
      mosq_rc = mosquitto_connect(m->mosq, mqtt_host, mqtt_port, keepalive);
      if (mosq_rc > 0)
      {
        _LOG("--- (f-c) c_mqtt_connect > mosquitto_connect failed: %d\n", mosq_rc);
        result = PL_resource_error("mqtt_connect_failed");
        goto CLEANUP;
      }
    }
    _LOG("--- (f-c) c_mqtt_connect > bind done\n");
  }

  if ( unify_swi_mqtt(conn, m) ) 
  {
    _LOG("--- (f-c) c_mqtt_connect > unify_swi_mqtt done\n");
    result = TRUE;
    goto CLEANUP;
  }



CLEANUP:
  PL_free(mqtt_host);
  PL_free(client_id);

  return result;
}

static foreign_t 
c_mqtt_loop(term_t conn)
{
  swi_mqtt *m;
  
  _LOG("--- (f-c) c_mqtt_loop\n");

  if (!get_swi_mqtt(conn, &m)) return FALSE;

  if (!m->is_async_loop_started)
  {
      if (mosquitto_loop_start(m->mosq) == MOSQ_ERR_SUCCESS)
      {
        m->is_async_loop_started = true;
        _LOG("--- (f-c) c_mqtt_loop > mosquitto_loop_start done\n");
      } else {
        _LOG("--- (f-c) c_mqtt_loop > mosquitto_loop_start failed\n");
        return PL_resource_error("mqtt_loop_start_failed2");
      }
  }

  if (mosquitto_loop(m->mosq, 10, 1) == MOSQ_ERR_SUCCESS)
  {
    return TRUE;
  }
  return FALSE;
}



// in options: [type(bin|char|double|int), qos(0|1|2), retain(true|false)]
static foreign_t 
c_mqtt_sub(term_t conn, term_t topic, term_t options)
{
  int result = FALSE;

  swi_mqtt *m;
  int mid;

  char* mqtt_topic   = NULL;
  int qos = 0;

  int mosq_rc;
  struct mosquitto *mosq;

  _LOG("--- (f-c) c_mqtt_sub\n");

  if (!get_swi_mqtt(conn, &m))
  {
    result = FALSE;
    goto CLEANUP;
  }

  mosq = m->mosq;
  _LOG("--- (f-c) c_mqtt_sub > have connection %p (mosq: %p)\n", m->mosq, mosq);

  mosq_rc = mosquitto_reconnect(mosq);
  if (mosq_rc == MOSQ_ERR_SUCCESS)
  {
    _LOG("--- (f-c) c_mqtt_sub > mosquitto_reconnect-ed\n");
  } else {
    _LOG("--- (f-c) c_mqtt_sub > mosquitto_reconnect failed: %d\n", mosq_rc);
  }


  if (!PL_get_chars(topic, &mqtt_topic, CVT_WRITE | BUF_MALLOC)) { 
    result = FALSE;
    goto CLEANUP;
  }
  _LOG("--- (f-c) c_mqtt_sub > topic is: %s\n", mqtt_topic);
/*
  if ( options )
  { 
      term_t tail = PL_copy_term_ref(options);
      term_t head = PL_new_term_ref();

      while(PL_get_list(tail, head, tail))
      { 
        size_t arity;
        atom_t name;

        if ( PL_get_name_arity(head, &name, &arity) && arity == 1 )
        { 
            term_t arg = PL_new_term_ref();

            _PL_get_arg(1, head, arg);
    
                   if ( name == ATOM_payload_type ) { if (!PL_get_chars(  arg, &payload_type, CVT_WRITE|BUF_MALLOC) ) { result = FALSE;goto CLEANUP; }
            } else if ( name == ATOM_qos          ) { if (!PL_get_integer(arg, &qos) ) { result = FALSE;goto CLEANUP; }
            }
    
        } else { 
            result = pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, head, "option");
            goto CLEANUP;
        }
      }
      // unify with NIL --> end of list
      if ( !PL_get_nil(tail) )
      { 
        result = pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, tail, "list");
        goto CLEANUP;
      }
  }
*/
  


  _LOG("--- (f-c) c_mqtt_sub > subscribe...\n");
  mosq_rc = mosquitto_subscribe(m->mosq, &mid, mqtt_topic, qos);
  if (mosq_rc == MOSQ_ERR_SUCCESS)
  {
    _LOG("--- (f-c) c_mqtt_sub > subscribe done\n");
    result = TRUE;
    goto CLEANUP;
  } else {
    _LOG("--- (f-c) c_mqtt_sub > subscribe failed rc: %d\n", mosq_rc);
  }


CLEANUP:
  PL_free(mqtt_topic);
  return result;
}



install_t 
install_mqtt(void)
{
  _LOG("--- (f-c) install_mqtt\n");

  ATOM_is_async       = PL_new_atom("is_async");
  ATOM_client_id      = PL_new_atom("client_id");
  ATOM_keepalive      = PL_new_atom("keepalive");
  ATOM_use_callbacks  = PL_new_atom("use_callbacks");

  ATOM_message_id     = PL_new_atom("message_id");
  ATOM_topic          = PL_new_atom("topic");
  ATOM_payload        = PL_new_atom("payload");
  ATOM_payload_len    = PL_new_atom("payload_len");
  ATOM_qos            = PL_new_atom("qos");
  ATOM_retain         = PL_new_atom("retain");

  ATOM_bind_address   = PL_new_atom("bind_address");

  ATOM_protocol_version = PL_new_atom("protocol_version");
  ATOM_v31            = PL_new_atom("v31");
  ATOM_v311           = PL_new_atom("v311");

//  ATOM_flow           = PL_new_atom("flow");
//  ATOM_foreign        = PL_new_atom("foreign");
//  ATOM_prolog         = PL_new_atom("prolog");

  ATOM_level          = PL_new_atom("level");
  ATOM_log            = PL_new_atom("log");

  ATOM_payload_type      = PL_new_atom("payload_type");
  ATOM_payload_type_raw  = PL_new_atom("payload_type_raw");
  ATOM_payload_type_char = PL_new_atom("payload_type_char");

  ATOM_result         = PL_new_atom("result");
  ATOM_reason          = PL_new_atom("reason");
  // now options (functors with arity 1)
  FUNCTOR_topic1      = PL_new_functor(ATOM_topic, 1);
  FUNCTOR_payload1    = PL_new_functor(ATOM_payload, 1);
  FUNCTOR_payload_len1= PL_new_functor(ATOM_payload_len, 1);
  FUNCTOR_qos1        = PL_new_functor(ATOM_qos, 1);
  FUNCTOR_retain1     = PL_new_functor(ATOM_retain, 1);
  FUNCTOR_message_id1 = PL_new_functor(ATOM_message_id, 1);

  FUNCTOR_level1      = PL_new_functor(ATOM_level, 1);
  FUNCTOR_log1        = PL_new_functor(ATOM_log, 1);

  FUNCTOR_result1     = PL_new_functor(ATOM_result, 1);
  FUNCTOR_reason1     = PL_new_functor(ATOM_reason, 1);

  // predicate to call
  PRED_on_connect2      = PL_predicate("mqtt_hook_on_connect",     2, "mqtt");
  PRED_on_disconnect2   = PL_predicate("mqtt_hook_on_disconnect",  2, "mqtt");

  PRED_on_log2          = PL_predicate("mqtt_hook_on_log",         2, "mqtt");
  PRED_on_message2      = PL_predicate("mqtt_hook_on_message",     2, "mqtt");

  PRED_on_publish2      = PL_predicate("mqtt_hook_on_publish",     2, "mqtt");
  PRED_on_subscribe2    = PL_predicate("mqtt_hook_on_subscribe",   2, "mqtt");
  PRED_on_unsubscribe2  = PL_predicate("mqtt_hook_on_unsubscribe", 2, "mqtt");

  // now foreign funcs
  PL_register_foreign("c_pack_version",    3, c_pack_version3,   0);
  PL_register_foreign("c_pack_version",    1, c_pack_version1,   0);
  PL_register_foreign("c_mqtt_version",    3, c_mqtt_version3,   0);
  PL_register_foreign("c_mqtt_version",    1, c_mqtt_version1,   0);
  PL_register_foreign("c_mqtt_loop",       1, c_mqtt_loop,       0);
  PL_register_foreign("c_mqtt_connect",    4, c_mqtt_connect,    0);
  PL_register_foreign("c_free_swi_mqtt",   1, c_free_swi_mqtt,   0);
  PL_register_foreign("c_mqtt_pub",        4, c_mqtt_pub,        0);
  PL_register_foreign("c_mqtt_disconnect", 1, c_mqtt_disconnect, 0);
  PL_register_foreign("c_mqtt_sub",        3, c_mqtt_sub,        0);
  //PL_register_foreign("c_mqtt_unsub",      3, c_mqtt_unsub,        0);

  signal(SIGINT, handle_signal);
  signal(SIGTERM, handle_signal);

  mosquitto_lib_init();
}


install_t 
uninstall_mqtt(void)
{
  _LOG("--- (f-c) uninstall_mqtt\n");
  // de-init mosquitto lib
  mosquitto_lib_cleanup();
}
