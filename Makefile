include $(GOROOT)/src/Make.inc

TARG=odbc
CGOFILES=\
	odbc.go\

GOFILES=\
	util.go\

ifeq ($(GOOS), windows)
CGO_LDFLAGS=-lodbc32	
else
CGO_LDFLAGS=-lodbc	
endif

CLEANFILES+=
CGO_OFILES+=

include $(GOROOT)/src/Make.pkg

%: install %.go util.o
	$(GC) $*.go
	$(LD) -o $@ $*.$O
