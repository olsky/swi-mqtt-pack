SOBJ=	$(PACKSODIR)/mqtt.$(SOEXT)
CFLAGS+=-std=c99
LIBS=	-lmosquitto

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
