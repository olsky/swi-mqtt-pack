MOSQUITTO_LIBS=-L../lib ../lib/libmosquitto.a
SOBJ=	$(PACKSODIR)/mqtt.$(SOEXT)
CFLAGS+= -Wall -g -O2 -std=c99 -fPIC
LIBS= -lrt -lm -lpthread  $(MOSQUITTO_LIBS)

all:	$(SOBJ)

$(SOBJ): c/mqtt.o
	mkdir -p $(PACKSODIR)
	$(LD) $(LDSOFLAGS) -o $@ $(SWISOLIB) $< $(LIBS)

check::
install::
clean:
	rm -f c/mqtt.o
distclean: clean
	rm -f $(SOBJ)
