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

#undef MQTT_DEBUG_FOREIGN
//#define MQTT_DEBUG_FOREIGN 1

#ifdef MQTT_DEBUG_FOREIGN
#define _LOG(...)    printf(__VA_ARGS__);fflush(stdout);
#else
#define _LOG(...) 
#endif

#define PACK_MAJOR 1
#define PACK_MINOR 0
#define PACK_REVISION 5
#define PACK_VERSION_NUMBER (PACK_MAJOR*10000+PACK_MINOR*100+PACK_REVISION)

#define MKATOM(n) ATOM_ ## n = PL_new_atom(#n);

static PL_engine_t current_engine;

static atom_t ATOM_is_async;
static atom_t ATOM_client_id;
static atom_t ATOM_keepalive;
//static atom_t ATOM_use_callbacks;

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

static atom_t ATOM_level;
static atom_t ATOM_log;

static atom_t ATOM_payload_type;
static atom_t ATOM_payload_type_raw;
static atom_t ATOM_payload_type_char;

static atom_t ATOM_result;
static atom_t ATOM_reason;

static atom_t ATOM_module;
static atom_t ATOM_on_connect;
static atom_t ATOM_on_disconnect;
static atom_t ATOM_on_log;
static atom_t ATOM_on_message;
static atom_t ATOM_on_publish;
static atom_t ATOM_on_subscribe;
static atom_t ATOM_on_unsubscribe;
static atom_t ATOM_hooks;
static atom_t ATOM_debug_hooks;

// these are options going with hook to prolog
static functor_t FUNCTOR_topic1;
static functor_t FUNCTOR_payload1;
static functor_t FUNCTOR_payload_len1;
static functor_t FUNCTOR_qos1;
static functor_t FUNCTOR_retain1;
static functor_t FUNCTOR_message_id1;

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
  bool          is_in_use;
  bool          is_async;               /* if true then use mosquitto_connect_async */
  int           refs;
  atom_t        symbol;                 /* <swi_mqtt>(%p) */
  bool          is_async_loop_started;  /* set to true after mosquitto_loop_start call */
  int           keepalive;
  int           loop_max_packets;
  PL_engine_t   pl_engine;              /* prolog engine for async processing */  
  bool          use_callbacks;
  predicate_t   callback_on_connect2;
  predicate_t   callback_on_disconnect2;
  predicate_t   callback_on_log2;
  predicate_t   callback_on_message2;
  predicate_t   callback_on_publish2;
  predicate_t   callback_on_subscribe2;
  predicate_t   callback_on_unsubscribe2;
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

static int
destroy_mqtt(swi_mqtt *m)
{ 
  _LOG("--- (f-b) destroy_mqtt swi_mqtt: %p refs: %d\n", m, m->refs);
  if (m && m->mosq && m->refs == 0) {
    if (m->is_in_use == TRUE)
    {
      _LOG("--- (f-b) destroy_mqtt - connection is in use\n");
      return FALSE;
    } else {
      _LOG("--- (f-b) destroy_mqtt - release mosq and blob\n");
      mosquitto_destroy(m->mosq);
      m->mosq = NULL;
      free(m);    
      m = NULL;
    }
  }
  return TRUE;
}


static int set_engine_for_callbacks(swi_mqtt *m)
{
  int thread_id = PL_thread_self();
  if (thread_id >= 0)
  {
    return TRUE;
  }
 
  PL_engine_t engine_to_use;

  if (m->is_async)
  {
    engine_to_use = m->pl_engine;
  } else {
    if (!current_engine)
    {
      current_engine = PL_create_engine(NULL);
    }
    engine_to_use = current_engine;
  }

  if (PL_set_engine(engine_to_use, NULL) == PL_ENGINE_SET)
  {
    return TRUE;
  }

  _LOG("--- (f-x) set_engine_for_callbacks > failed to recover an engine. probably mqtt_disconnect was called...\n");
  return FALSE;  
}

/*
static int unset_engine_for_callbacks(swi_mqtt *m)
{
  if (m->is_async && PL_set_engine(NULL, NULL) == PL_ENGINE_SET)
  {
    return TRUE;
  }
  return FALSE; 
}
*/

int have_thread_engine(swi_mqtt *m)
{
  int thread_id = PL_thread_self();
  if (thread_id < 0)
  { 
    if (set_engine_for_callbacks(m) == FALSE)
    {
      return FALSE;
    } else {
      thread_id = PL_thread_self();
      _LOG("--- (f-x) have_thread_engine > got on: %d\n", thread_id);
    }
  } else {
    _LOG("--- (f-x) have_thread_engine > have on: %d\n", thread_id);
  }
  return TRUE;
}

		 /*******************************
		 *	      CALLBACKS	            *
		 *******************************/

#define CB_PREPARE_FRAME()   fid_t fid = PL_open_foreign_frame();predicate_t callback_to_use;term_t t0 = PL_new_term_refs(2);term_t t1 = t0 + 1;unify_swi_mqtt(t0, m);

#define CB_CALL_N_CLOSE_FRAME(cb_sufx) if (m->use_callbacks == TRUE){callback_to_use = m->callback_##cb_sufx##2;} else {callback_to_use = PRED_##cb_sufx##2;} \
  PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, callback_to_use, t0);PL_discard_foreign_frame(fid);

void on_log_callback(struct mosquitto *mosq, void *obj, int level, const char *str)
{
  swi_mqtt *m = (swi_mqtt *)obj;
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_log > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()


  add_int_option( t1, FUNCTOR_level1,  level);
  add_char_option(t1, FUNCTOR_log1,    str);
  close_list(t1);

  CB_CALL_N_CLOSE_FRAME(on_log)

  // unset_engine_for_callbacks(m);
}


void on_connect_callback(struct mosquitto *mosq, void *obj, int result)
{
  swi_mqtt *m = (swi_mqtt *)obj;
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_connect > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()

  add_int_option( t1, FUNCTOR_result1,  result);
  close_list(t1);

  CB_CALL_N_CLOSE_FRAME(on_connect)
}

void on_disconnect_callback(struct mosquitto *mosq, void *obj, int reason)
{
  swi_mqtt *m = (swi_mqtt *)obj;
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_disconnect > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()

  add_int_option( t1, FUNCTOR_reason1,  reason);
  close_list(t1);

  CB_CALL_N_CLOSE_FRAME(on_disconnect)

  if (m->is_async)
  {
    if (PL_destroy_engine(m->pl_engine) == FALSE)
    {
      _LOG("--- (f-c) on_disconnect_callback > unable to destroy pl_engine %p\n", m->pl_engine);          
    }
  }

}

void on_message_callback(struct mosquitto *mosq, void *obj, const struct mosquitto_message *message)
{
  swi_mqtt *m = (swi_mqtt *)obj;
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_message > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()

  add_int_option( t1, FUNCTOR_message_id1,  message->mid);
  add_char_option(t1, FUNCTOR_topic1,       message->topic);
  add_char_option(t1, FUNCTOR_payload1,     message->payload);
  add_int_option( t1, FUNCTOR_payload_len1, message->payloadlen);
  add_int_option( t1, FUNCTOR_qos1,         message->qos);
  add_int_option( t1, FUNCTOR_retain1,      message->retain);
  close_list(t1);

  CB_CALL_N_CLOSE_FRAME(on_message)
}

void on_publish_callback(struct mosquitto *mosq, void *obj, int mid)
{
  swi_mqtt *m = (swi_mqtt *)obj;
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_publish > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()

  add_int_option( t1, FUNCTOR_message_id1,  mid);
  close_list(t1);
  
  CB_CALL_N_CLOSE_FRAME(on_publish)
}

void on_subscribe_callback(struct mosquitto *mosq, void *obj, int mid, int qos_count, const int *granted_qos)
{
  swi_mqtt *m = (swi_mqtt *)obj;
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_subscribe > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()

  add_int_option( t1, FUNCTOR_message_id1,  mid);
  //add_int_option(t1, FUNCTOR_log1,    qos_count);
  close_list(t1);

  CB_CALL_N_CLOSE_FRAME(on_subscribe)
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
  //predicate_t callback_to_use;

  _LOG("--- (f-x) on_unsubscribe > mosq: %p\n", mosq);

  if (have_thread_engine(m) == FALSE)
  { 
    return;
  }

  CB_PREPARE_FRAME()

  add_int_option( t1, FUNCTOR_message_id1,  mid);
  close_list(t1);

  CB_CALL_N_CLOSE_FRAME(on_unsubscribe)
}






		 /*******************************
		 *	      SYMBOL                *
		 *******************************/

static void
acquire_mqtt_symbol(atom_t symbol)
{ 
  _LOG("--- (f-b) acquire_mqtt_symbol symbol: %d\n", (int) symbol);

  swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  m->symbol = symbol;
  m->refs++;
}

static int
release_mqtt_symbol(atom_t symbol)
{ 
  _LOG("--- (f-b) release_mqtt_symbol symbol: %d\n", (int) symbol);

  swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  m->refs--;
  destroy_mqtt(m);
  return TRUE;
}

static int
compare_mqtt_symbols(atom_t a, atom_t b)
{ 
  _LOG("--- (f-b) compare_mqtt_symbols\n");

  swi_mqtt *ma = PL_blob_data(a, NULL, NULL);
  swi_mqtt *mb = PL_blob_data(b, NULL, NULL);
  return ( ma > mb ?  1 :
	   ma < mb ? -1 : 0
	 );
}

static int
write_mqtt_symbol(IOSTREAM *s, atom_t symbol, int flags)
{ 
  _LOG("--- (f-b) write_mqtt_symbol\n");

  swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  // Sfprintf(s, "<swi_mqtt>(%p-%p)", m, m->mosq);
  Sfprintf(s, "<swi_mqtt_mosq>(%p)", m->mosq);
  return TRUE;
}

static PL_blob_t mqtt_blob =
{ PL_BLOB_MAGIC,
  PL_BLOB_UNIQUE | PL_BLOB_NOCOPY,
  "mqtt_connection",
  release_mqtt_symbol,
  compare_mqtt_symbols,
    write_mqtt_symbol,
  acquire_mqtt_symbol
};

static int
unify_swi_mqtt(term_t handle, swi_mqtt *m)
{
  _LOG("--- (f-b) unify_swi_mqtt\n");
  
  if ( PL_unify_blob(handle, m, sizeof(*m), &mqtt_blob) )
    return TRUE;
  if ( !PL_is_variable(handle) )
    return PL_uninstantiation_error(handle);
  return FALSE;					/* (resource) error */
}

static int
get_swi_mqtt(term_t handle, swi_mqtt **mp)
{ PL_blob_t *type;

  _LOG("--- (f-b) get_swi_mqtt\n");

  void *data;
  if ( PL_get_blob(handle, &data, NULL, &type) && type == &mqtt_blob)
  { swi_mqtt *m = data;
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
  _LOG("--- (f-b) release_swi_mqtt\n");
}


/*
static void
empty_swi_mqtt(swi_mqtt *m)
{ if ( m->data )
    free(m->data);

  // init with default values
  m->symbol = 0;
  m->is_in_use = FALSE;
  m->is_async              = TRUE;
  m->is_async_loop_started = FALSE;
  m->keepalive             = 60;
  m->loop_max_packets      = 10;
  m->pl_engine             = -1;
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
    m->is_in_use = FALSE;
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
      m->is_in_use = FALSE;
      if (m->is_async)
      {
        if (PL_destroy_engine(m->pl_engine) == FALSE)
        {
          _LOG("--- (f-c) c_mqtt_disconnect > unable to destroy pl_engine %p\n", m->pl_engine);          
        }

      }
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
  //int buf_len = 2048;
  //char buf[buf_len];
  char* mqtt_topic   = NULL;
  char* mqtt_payload = NULL;
  char* payload_type = NULL;
  int qos = 0;
  int retain = 0;

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

  if ( options )
  { 
      _LOG("--- (f-c) c_mqtt_pub > parsing options...\n");
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
    
                   if ( name == ATOM_retain) { if (!PL_get_bool(   arg, &retain)) { result = FALSE;_LOG("--- (f-c) c_mqtt_pub > get ATOM_retain failed\n");goto CLEANUP;}
            } else if ( name == ATOM_qos )   { if (!PL_get_integer(arg, &qos)   ) { result = FALSE;_LOG("--- (f-c) c_mqtt_pub > get ATOM_qos failed\n");goto CLEANUP;}
            }
    
        } else { 
            _LOG("--- (f-c) c_mqtt_pub > PL_get_name_arity failed\n");
            result = FALSE; // pl_error("c_mqtt_pub", 4, NULL, ERR_TYPE, head, "option");
            goto CLEANUP;
        }
      }
      // unify with NIL --> end of list
      if ( !PL_get_nil(tail) )
      { 
        _LOG("--- (f-c) c_mqtt_pub > PL_get_nil failed\n");
        result = FALSE; // pl_error("c_mqtt_pub", 4, NULL, ERR_TYPE, tail, "list");
        goto CLEANUP;
      }
      _LOG("--- (f-c) c_mqtt_pub > parsing options done\n");
  }

  if (!PL_get_chars(payload, &mqtt_payload, CVT_WRITE | BUF_MALLOC)) { 
    result = FALSE;
    goto CLEANUP;
  }

  _LOG("--- (f-c) c_mqtt_pub > qos: %d retain: %d payload: %s\n", qos, retain, mqtt_payload);

  
  //memset(buf, 0, (buf_len+1)*sizeof(char));
  //snprintf(buf, buf_len, "%s", mqtt_payload);

  _LOG("--- (f-c) c_mqtt_pub > publish...\n");
  // mosq_rc = mosquitto_publish(m->mosq, &mid, mqtt_topic, strlen(buf), buf, qos, retain);
  mosq_rc = mosquitto_publish(m->mosq, &mid, mqtt_topic, strlen(mqtt_payload), mqtt_payload, qos, retain);
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
  int use_hooks = FALSE;
  int use_debug_hooks = FALSE;

  char* hook_module = NULL;
  char* hook_on_log = NULL;
  char* hook_on_connect = NULL;
  char* hook_on_disconnect = NULL;
  char* hook_on_message = NULL;
  char* hook_on_publish = NULL;
  char* hook_on_subscribe = NULL;
  char* hook_on_unsubscribe = NULL;



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
	  
  	        if        ( name == ATOM_client_id )    { if (!PL_get_chars(  arg, &client_id,          CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}

            } else if ( name == ATOM_module )       { if (!PL_get_chars(  arg, &hook_module,        CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}
            } else if ( name == ATOM_on_log )       { if (!PL_get_chars(  arg, &hook_on_log,        CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}

            } else if ( name == ATOM_on_connect)    { if (!PL_get_chars(  arg, &hook_on_connect,    CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}
            } else if ( name == ATOM_on_disconnect) { if (!PL_get_chars(  arg, &hook_on_disconnect, CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}

            } else if ( name == ATOM_on_message )   { if (!PL_get_chars(  arg, &hook_on_message,    CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}
            } else if ( name == ATOM_on_publish )   { if (!PL_get_chars(  arg, &hook_on_publish,    CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}

            } else if ( name == ATOM_on_subscribe)  { if (!PL_get_chars(  arg, &hook_on_subscribe,  CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}
            } else if ( name == ATOM_on_unsubscribe){ if (!PL_get_chars(  arg, &hook_on_unsubscribe,CVT_WRITE | BUF_MALLOC) ) { result = FALSE; goto CLEANUP;}

            } else if ( name == ATOM_hooks )        { if (!PL_get_bool(   arg, &use_hooks)       ) { result = FALSE;goto CLEANUP;}
            } else if ( name == ATOM_debug_hooks)   { if (!PL_get_bool(   arg, &use_debug_hooks) ) { result = FALSE;goto CLEANUP;}
            } else if ( name == ATOM_is_async )     { if (!PL_get_bool(   arg, &is_async)        ) { result = FALSE;goto CLEANUP;}
  	        } else if ( name == ATOM_keepalive )    { if (!PL_get_integer(arg, &keepalive)       ) { result = FALSE; goto CLEANUP;}
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
  
  m->refs = 0;
  m->use_callbacks = FALSE;
  m->is_async = is_async;
  m->is_async_loop_started = false;
  m->keepalive = keepalive;

  // pass swi_mqtt as user object (will be available in all callbacks)
  mosq = mosquitto_new(client_id, true, m);
  if (mosq)
  {
    m->is_in_use = TRUE;
    _LOG("--- (f-c) c_mqtt_connect > new mqtt: %p\n", mosq);
    m->mosq	= mosq;
    _LOG("--- (f-c) c_mqtt_connect > m->mosq: %p\n", m->mosq);

    // if ATOM_use_callbacks set -->
    _LOG("--- (f-c) c_mqtt_connect > hooks: %d debug_hooks: %d\n", use_hooks, use_debug_hooks);
    // set all callbacks
    if (use_hooks)
    {
      _LOG("--- (f-c) c_mqtt_connect > setting custom hooks in module: %s\n", hook_module);
      m->use_callbacks = TRUE;
      if (hook_on_log)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_log: %s\n", hook_on_log);
        m->callback_on_log2 = PL_predicate(hook_on_log,     2, hook_module);  
        mosquitto_log_callback_set(m->mosq, on_log_callback);
      }
      
      if (hook_on_connect)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_connect: %s\n", hook_on_connect);
        m->callback_on_connect2 = PL_predicate(hook_on_connect,     2, hook_module);  
        mosquitto_connect_callback_set(m->mosq, on_connect_callback);
      }
      if (hook_on_disconnect)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_disconnect: %s\n", hook_on_disconnect);
        m->callback_on_disconnect2 = PL_predicate(hook_on_disconnect,     2, hook_module);  
        mosquitto_disconnect_callback_set(m->mosq, on_disconnect_callback);
      }

      if (hook_on_message)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_message: %s\n", hook_on_message);
        m->callback_on_message2 = PL_predicate(hook_on_message,     2, hook_module);  
        mosquitto_message_callback_set(m->mosq, on_message_callback);
      }

      if (hook_on_publish)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_publish: %s\n", hook_on_publish);
        m->callback_on_publish2 = PL_predicate(hook_on_publish,     2, hook_module);  
        mosquitto_publish_callback_set(m->mosq, on_publish_callback);
      }

      if (hook_on_subscribe)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_subscribe: %s\n", hook_on_subscribe);
        m->callback_on_subscribe2 = PL_predicate(hook_on_subscribe,     2, hook_module);  
        mosquitto_subscribe_callback_set(m->mosq, on_subscribe_callback);
      }
      if (hook_on_unsubscribe)
      {
        _LOG("--- (f-c) c_mqtt_connect > setting custom hook on_unsubscribe: %s\n", hook_on_unsubscribe);
        m->callback_on_unsubscribe2 = PL_predicate(hook_on_unsubscribe,     2, hook_module);  
        mosquitto_unsubscribe_callback_set(m->mosq, on_unsubscribe_callback);
      }

    } else if (use_debug_hooks)
    {
      _LOG("--- (f-c) c_mqtt_connect > setting debug hooks\n");
      mosquitto_log_callback_set(m->mosq, on_log_callback);

      mosquitto_connect_callback_set(m->mosq, on_connect_callback);
      mosquitto_disconnect_callback_set(m->mosq, on_disconnect_callback);

      mosquitto_message_callback_set(m->mosq, on_message_callback);
      mosquitto_publish_callback_set(m->mosq, on_publish_callback);

      mosquitto_subscribe_callback_set(m->mosq, on_subscribe_callback);
      mosquitto_unsubscribe_callback_set(m->mosq, on_unsubscribe_callback);
    }

    
    if (is_async) {
      _LOG("--- (f-c) c_mqtt_connect > create prolog engine for async\n");
      m->pl_engine = PL_create_engine(NULL);

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
  } else {
    _LOG("--- (f-c) c_mqtt_connect > unable to create mosq client\n");
    result = FALSE;
    goto CLEANUP;    
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

  if (m->is_async && !m->is_async_loop_started)
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





static foreign_t 
c_mqtt_unsub(term_t conn, term_t topic)
{
  int result = FALSE;

  swi_mqtt *m;
  // int mid;

  char* mqtt_topic   = NULL;

  int mosq_rc;
  struct mosquitto *mosq;

  _LOG("--- (f-c) c_mqtt_unsub\n");

  if (!get_swi_mqtt(conn, &m))
  {
    result = FALSE;
    goto CLEANUP;
  }

  mosq = m->mosq;
  _LOG("--- (f-c) c_mqtt_unsub > have connection %p (mosq: %p)\n", m->mosq, mosq);

  if (!PL_get_chars(topic, &mqtt_topic, CVT_WRITE | BUF_MALLOC)) { 
    result = FALSE;
    goto CLEANUP;
  }
  _LOG("--- (f-c) c_mqtt_unsub > topic pattern is: %s\n", mqtt_topic);

  mosq_rc = mosquitto_unsubscribe(mosq, NULL, mqtt_topic);
  if (mosq_rc == MOSQ_ERR_SUCCESS)
  {
    result = TRUE;
    _LOG("--- (f-c) c_mqtt_sub > un-sub-ed\n");
  } else {
    _LOG("--- (f-c) c_mqtt_sub > unsubscribe failed: %d\n", mosq_rc);
  }

CLEANUP:
  PL_free(mqtt_topic);
  return result;
}

// is this required?
static foreign_t 
c_create_engine(void)
{
  if (!current_engine)
  {
    current_engine =  PL_create_engine(NULL); // PL_current_engine();
    return TRUE;
  }
  return FALSE;
}

static foreign_t 
c_destroy_engine(void)
{
  if (current_engine)
  {
    return PL_destroy_engine(current_engine);
  }
  return FALSE;
}

     /*******************************
     *        install / uninstall   *
     *******************************/


install_t 
install_mqtt(void)
{
  _LOG("--- (f-c) install_mqtt\n");

  mosquitto_lib_init();

  ATOM_is_async       = PL_new_atom("is_async");
  ATOM_client_id      = PL_new_atom("client_id");
  ATOM_keepalive      = PL_new_atom("keepalive");
  //ATOM_use_callbacks  = PL_new_atom("use_callbacks");

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

  ATOM_level          = PL_new_atom("level");
  ATOM_log            = PL_new_atom("log");

  ATOM_payload_type      = PL_new_atom("payload_type");
  ATOM_payload_type_raw  = PL_new_atom("payload_type_raw");
  ATOM_payload_type_char = PL_new_atom("payload_type_char");

  ATOM_result          = PL_new_atom("result");
  ATOM_reason          = PL_new_atom("reason");


  ATOM_module           = PL_new_atom("module");
  ATOM_on_connect       = PL_new_atom("on_connect");
  ATOM_on_disconnect    = PL_new_atom("on_disconnect");
  ATOM_on_log           = PL_new_atom("on_log");
  ATOM_on_message       = PL_new_atom("on_message");
  ATOM_on_publish       = PL_new_atom("on_publish");
  ATOM_on_subscribe     = PL_new_atom("on_subscribe");
  ATOM_on_unsubscribe   = PL_new_atom("on_unsubscribe");

  ATOM_hooks            = PL_new_atom("hooks");
  ATOM_debug_hooks      = PL_new_atom("debug_hooks");

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
  PL_register_foreign("c_mqtt_connect",    4, c_mqtt_connect,    0);
  PL_register_foreign("c_mqtt_disconnect", 1, c_mqtt_disconnect, 0);
  PL_register_foreign("c_mqtt_pub",        4, c_mqtt_pub,        0);
  PL_register_foreign("c_mqtt_sub",        3, c_mqtt_sub,        0);
  PL_register_foreign("c_mqtt_loop",       1, c_mqtt_loop,       0);
  PL_register_foreign("c_mqtt_unsub",      2, c_mqtt_unsub,      0);

  PL_register_foreign("c_free_swi_mqtt",   1, c_free_swi_mqtt,   0);

  PL_register_foreign("c_create_engine",   0, c_create_engine,   0);
  PL_register_foreign("c_destroy_engine",  0, c_destroy_engine,  0);

  // signal(SIGINT, handle_signal);
  // signal(SIGTERM, handle_signal);
}


install_t 
uninstall_mqtt(void)
{
  _LOG("--- (f-c) uninstall_mqtt\n");
  // de-init mosquitto lib
  mosquitto_lib_cleanup();
}
