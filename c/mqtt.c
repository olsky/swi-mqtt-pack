/*
* MQTT foreign library
* uses mosquitto
*
*/

#include <stdio.h>

#include <SWI-Prolog.h>
#include <mosquitto.h>


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

static atom_t ATOM_flow;
static atom_t ATOM_foreign;
static atom_t ATOM_prolog;

static atom_t ATOM_level;
static atom_t ATOM_log;

// these are options going with hook to prolog
static functor_t FUNCTOR_topic1;
static functor_t FUNCTOR_payload1;
static functor_t FUNCTOR_payload_len;
static functor_t FUNCTOR_qos1;
static functor_t FUNCTOR_retain1;
static functor_t FUNCTOR_message_id1;

static functor_t FUNCTOR_flow1;
static functor_t FUNCTOR_level1;
static functor_t FUNCTOR_log1;


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
  
  
static int
add_int_option(term_t list, functor_t f, int value)
{
  // type is of: PL_INTEGER

  int result = FALSE;

  term_t tail = PL_copy_term_ref(list);
  term_t head = PL_new_term_ref();

  while(PL_get_list(tail, head, tail))
  { if ( PL_unify_functor(head, f) )
    { term_t a = PL_new_term_ref();

      if ( !PL_get_arg(1, head, a)) return FALSE;

      switch (type)
      {
       case PL_INTEGER: PL_unify_integer(a, value); break;
       case PL_ATOM   : PL_unify_atom(a, value); break;
       case PL_INTEGER: PL_unify_integer(a, value); break;

      default:
	return FALSE;
      }
    
      return rezult;
    }
  }

  if ( PL_unify_list(tail, head, tail) )
    return PL_unify_term(head, PL_FUNCTOR, f, type, value);

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








typedef struct
{
  mosquitto    *mosq;                   /* mosquito structure */
  bool          is_async;               /* if true then use mosquitto_connect_async */
  atom_t	symbol;			/* <swi_mqtt>(%p) */
  bool          is_async_loop_started;  /* set to true after mosquitto_loop_start call */
  int           keepalive;
  int           loop_max_packets;

} swi_mqtt;




static int
destroy_mqtt(swi_mqtt *m)
{ 
  mosquitto_destroy(m->mosq);
  
  free(m);

  return TRUE;
}



		 /*******************************
		 *	      CALLBACKS		*
		 *******************************/


void on_connect_callback(struct mosquitto *mosq, void *obj, int result)
{
  // struct swi_mqtt swi_mqtt = (struct swi_mqtt *)obj;
}

void on_message_callback(struct mosquitto *mosq, void *obj, const struct mosquitto_message *message)
{
  struct swi_mqtt m = (struct swi_mqtt *)obj;



// message->topic;
// message->payload; message->payloadlen
/*

struct mosquitto_message{
	int mid;
	char *topic;
	void *payload;
	int payloadlen;
	int qos;
	bool retain;
};


*/
 
  fid_t fid = PL_open_foreign_frame();
  
  term_t conn; 
  unify_swi_mqtt(conn, m)
  
  term_t t0 = PL_new_term_refs(2);
  
  
  if ( PL_call_predicate(NULL, PL_Q_PASS_EXCEPTION, PRED_on_message2, t0)
  {
    // NO_OP
  }
  
  PL_discard_foreign_frame(fid);
}

void on_publish_callback(struct mosquitto *mosq, void *obj, int mid)
{

}


		 /*******************************
		 *	      SYMBOL		*
		 *******************************/

static void
acquire_mqtt_symbol(atom_t symbol)
{ swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);
  m->symbol = symbol;
}

static int
release_mqtt_symbol(atom_t symbol)
{ swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);

  destroy_mqtt(m);
  return TRUE;
}

static int
compare_mqtt_symbols(atom_t a, atom_t b)
{ swi_mqtt *ma = PL_blob_data(a, NULL, NULL);
  swi_mqtt *mb = PL_blob_data(b, NULL, NULL);

  return ( ma > mb ?  1 :
	   ma < mb ? -1 : 0
	 );
}


static int
write_mqtt_symbol(IOSTREAM *s, atom_t symbol, int flags)
{ swi_mqtt *m = PL_blob_data(symbol, NULL, NULL);

  Sfprintf(s, "<swi_mqtt>(%p)", m);
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
{ if ( PL_unify_blob(handle, m, sizeof(*m), &mqtt_blob) )
    return TRUE;

  if ( !PL_is_variable(handle) )
    return PL_uninstantiation_error(handle);

  return FALSE;					/* (resource) error */
}

static int
get_swi_mqtt(term_t handle, swi_mqtt **mp)
{ PL_blob_t *type;
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
{ UNLOCK(m);
}


static void
empty_swi_mqtt(swi_mqtt *m)
{ if ( m->data )
    free(m->data);

  m->encoding     = ENC_UTF8;
  m->data         = NULL;
  m->end          = 0;
  m->gap_start    = 0;
  m->gap_size     = 0;
  m->char_count   = NOSIZE;
  m->pcache.valid = 0;
  m->here         = 0;
}







// TODO add following:
// - define blob to wrap: static struct mosquitto *mosq = NULL;
// - init mosquitto lib (mosquitto_new)
// - cleanup (mosquitto_lib_cleanup)
// - connect/disconnect to/from broker (mosquitto_connect_bind, mosquitto_disconnect)
// - current connections
// - connection discovery predicate
// - publish predicate (mosquitto_publish)
// - subscribe predicate (mosquitto_subscribe)




static foreign_t
c_free_swi_mqtt(term_t handle)
{ swi_mqtt *m;

  if ( get_swi_mqtt(handle, &m) )
  {
    m->symbol = 0;
  
    mosquitto_destroy(m->mosq);
    
    release_mqtt(m);
    return TRUE;
  }

  return FALSE;
}






static foreign_t 
c_mqtt_version(term_t ver)
{
  term_t tmp = PL_new_term_ref();

  if ( 
      PL_unify_term(tmp,PL_FUNCTOR_CHARS,".",2,PL_INT, LIBMOSQUITTO_MINOR, PL_INT, LIBMOSQUITTO_REVISION) // Minor + Rev 
      &&
      PL_unify_term(ver,PL_FUNCTOR_CHARS,".",2,PL_INT, LIBMOSQUITTO_MAJOR, PL_TERM, tmp )    // Major
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
  
  if (get_swi_mqtt(conn, &m) && mosquitto_disconnect(m->mosq))
  {
    return TRUE;
  }

  return FALSE;
}


static foreign_t 
c_mqtt_loop(term_t conn)
{
  swi_mqtt *m;
  
  if (get_swi_mqtt(conn, &m) && mosquitto_loop(m->mosq, 10, 1))
  {
    return TRUE;
  }

  return FALSE;
}


// in options: [type(bin|char|double|int), qos(0|1|2), retain(true|false)]
static foreign_t 
c_mqtt_pub(term_t conn, term_t topic, term_t payload, term_t options)
{
  swi_mqtt *m;
  int mid;
  char buf[100];

  if (!get_swi_mqtt(conn, &m))
  {
    return FALSE;
  }

  if (mosquitto_publish(mosq, &mid, mqtt_topic, strlen(buf), buf, qos, retain);)
  {
    return TRUE;
  }

  return FALSE;
}


static foreign_t 
c_mqtt_connect(term_t conn, term_t host, term_t port, term_t options)
{
  struct mosquitto *mosq;

  int rc = 0;

  char* mqtt_host = NULL;
  int mqtt_port = 1883;

  char* client_id;
  int keepalive = 60;
  bool is_async = FALSE;
  
  if ( !PL_get_integer_ex(port, &mqtt_port))
    return FALSE;
    

  if (!PL_get_chars(host, &mqtt_host, CVT_WRITE | BUF_MALLOC)) { 
    PL_free(mqtt_host);
    return FALSE;
  }
    
  if ( options )
  { 
      term_t tail = PL_copy_term_ref(options);
      term_t head = PL_new_term_ref();

      while(PL_get_list(tail, head, tail))
      { size_t arity;
	atom_t name;

	if ( PL_get_name_arity(head, &name, &arity) && arity == 1 )
	{ term_t arg = PL_new_term_ref();

	  _PL_get_arg(1, head, arg);
	  
	  if        ( name == ATOM_client_id ) { if (!get_encoding(  arg, &client_id) ) { return FALSE; }
	  } else if ( name == ATOM_is_async )  { if (!PL_get_bool(   arg, &is_async)  ) { return pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, arg, "boolean");}
	  } else if ( name == ATOM_keepalive ) { if (!PL_get_long_ex(arg, &keepalive) ) { return FALSE; }
	  }
	  
	} else { 
	  return pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, head, "option");
	}
      }
      if ( !PL_get_nil(tail) )
      { 
        return pl_error("c_mosquitto_connect", 4, NULL, ERR_TYPE, tail, "list");
      }
  }


  // allocate mem for struct
  swi_mqtt *m = calloc(1, sizeof(*m));

  if ( !m )
    return PL_resource_error("memory");
  
  // pass swi_mqtt as user object (will be available in all callbacks)
  mosq = mosquitto_new(client_id, true, m);
  if (mosq)
  {
    m->mosq	= mosq;
    m->is_async	= is_async;
    m->is_async_loop_started = false;

    // if ATOM_use_callbacks set -->
    // set all callbacks
    mosquitto_connect_callback_set(mosq, on_connect_callback);
    mosquitto_message_callback_set(mosq, on_message_callback);
    mosquitto_publish_callback_set(mosq, on_publish_callback);

    
    if (is_async) {
      rc = mosquitto_connect_bind_async(mosq, mqtt_host, mqtt_port, keepalive, NULL);
    } else {
      // call it for sync:
      mosquitto_threaded_set(mosq, true);
      rc = mosquitto_connect_bind(mosq, mqtt_host, mqtt_port, keepalive, NULL);
    }
    if (rc > 0)
    {
      return PL_resource_error("mqtt_connect_failed");
    }
  }

  if ( unify_swi_mqtt(conn, m) )
    return TRUE;

  return FALSE;
}


install_t 
install_mqtt(void)
{
  // MKATOM(client_id);
  ATOM_client_id	    = PL_new_atom("client_id");
  ATOM_keepalive	    = PL_new_atom("keepalive");
  ATOM_is_async	   	    = PL_new_atom("is_async");
  ATOM_message_id   	    = PL_new_atom("message_id");

  ATOM_bind_address   	    = PL_new_atom("bind_address");

  ATOM_protocol_version     = PL_new_atom("protocol_version");
  ATOM_v31   	    	    = PL_new_atom("v31");
  ATOM_v311   	    	    = PL_new_atom("v311");
  ATOM_topic   	    	    = PL_new_atom("topic");
  

  // now options (functors with arity 1)
  FUNCTOR_topic1 = PL_new_functor(ATOM_topic, 1);


  // predicate to call
  PRED_on_log2        = PL_predicate("mqtt_hook_on_log",     2, "mqtt");
  PRED_on_message2    = PL_predicate("mqtt_hook_on_message", 2, "mqtt");

  // now foreign funcs
  PL_register_foreign("c_mqtt_version",    1, c_mqtt_version,    0);
  PL_register_foreign("c_mqtt_loop",       3, c_mqtt_loop,       0);
  PL_register_foreign("c_mqtt_connect",    4, c_mqtt_connect,    0);
  PL_register_foreign("c_free_swi_mqtt",   1, c_free_swi_mqtt,   0);
  PL_register_foreign("c_mqtt_pub",        4, c_mqtt_pub,        0);
  PL_register_foreign("c_mqtt_disconnect", 1, c_mqtt_disconnect, 0);




  mosquitto_lib_init();
}


install_t 
uninstall_mqtt(void)
{
  // de-init mosquitto lib
  mosquitto_lib_cleanup();
}
