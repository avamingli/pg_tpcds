EXTENSION = tpcds
DATA = tpcds--1.0.sql
REGRESS = gen_schema gen_query explain

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

install: build-dsgen install-tpcds-dsgen

build-dsgen:
	$(MAKE) -C DSGen-software-code-4.0.0/tools

install-tpcds-dsgen:
	mkdir -p $(DESTDIR)$(datadir)/extension/tpcds_dsgen
	cp -r DSGen-software-code-4.0.0/* $(DESTDIR)$(datadir)/extension/tpcds_dsgen/

clean: clean-dsgen

clean-dsgen:
	$(MAKE) -C DSGen-software-code-4.0.0/tools clean


uninstall: uninstall-tpcds-dsgen

uninstall-tpcds-dsgen:
	psql -c "DROP EXTENSION IF EXISTS tpcds CASCADE; DROP SCHEMA IF EXISTS tpcds CASCADE;"
	rm -rf $(DESTDIR)$(datadir)/extension/tpcds_dsgen
	rm -f $(DESTDIR)$(datadir)/extension/tpcds.control
	rm -f $(DESTDIR)$(datadir)/extension/tpcds--1.0.sql
