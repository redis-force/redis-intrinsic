# find the OS
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')

# Compile flags for linux / osx
ifeq ($(uname_S),Linux)
	SHOBJ_CFLAGS ?= -D_POSIX_SOURCE -D_BSD_SOURCE -W -Wall -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_LDFLAGS ?= -shared
else
	SHOBJ_CFLAGS ?= -W -Wall -dynamic -fno-common -g -ggdb -std=c99 -O2
	SHOBJ_LDFLAGS ?= -bundle -undefined dynamic_lookup
endif

.SUFFIXES: .c .so .xo .o

all: intrinsic.so

.c.xo:
	$(CC) -I. $(CFLAGS) $(SHOBJ_CFLAGS) -fPIC -c $< -o $@

RPM_OBJECTS = intrinsic.xo

intrinsic.xo: redismodule.h

intrinsic.so: $(RPM_OBJECTS)
	$(LD) -o $@ $(RPM_OBJECTS) $(SHOBJ_LDFLAGS) $(LIBS)

clean:
	rm -rf *.xo *.so
