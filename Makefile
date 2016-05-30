PACKSODIR=lib/x86_64-linux
MOSQUITTO_LIBS=-Lext ext/libmosquitto.a
SO=so
SOEXT=so
SOBJ=$(PACKSODIR)/mqtt.$(SOEXT)
COFLAGS=-O2 -g
CWFLAGS=-Wall
CMFLAGS=-fno-strict-aliasing -pthread -fPIC -std=c99 
CIFLAGS=-I. -Iext/include
DEFS=
CFLAGS=$(COFLAGS) $(CWFLAGS) $(CMFLAGS) $(CIFLAGS) $(PKGCFLAGS) $(DEFS)
CXXFLAGS=$(CFLAGS)

LD=gcc

PLLDFLAGS=-rdynamic -O2 -g -Lext -pthread -Wl,-rpath=ext -Lext -lrt -lm -lpthread  
LIBS=  $(MOSQUITTO_LIBS)
LDSOFLAGS=-shared $(PLLDFLAGS) 

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
