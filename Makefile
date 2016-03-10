include buildconf.mk

all: syndicate

syndicate: ag

.PHONY: ag
ag:
	$(MAKE) -C ag

.PHONY: install
install:
	$(MAKE) -C ag install

.PHONY: uninstall
uninstall:
	$(MAKE) -C ag uninstall

.PHONY: clean
clean:
	$(MAKE) -C ag clean

