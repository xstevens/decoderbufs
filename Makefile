MODULE_big = decoderbufs
EXTENSION = decoderbufs

PROTOBUF_C_CFLAGS = $(shell pkg-config --cflags 'libprotobuf-c >= 1.0.0')
PROTOBUF_C_LDFLAGS = $(shell pkg-config --libs 'libprotobuf-c >= 1.0.0')

PG_CPPFLAGS += -std=c11 -Wno-declaration-after-statement -Werror $(PROTOBUF_C_CFLAGS) -Isrc/
SHLIB_LINK  += $(PROTOBUF_C_LDFLAGS) -lz

ifeq ($(WITH_POSTGIS),1)
PG_CPPFLAGS += -DWITH_POSTGIS=1 -I/usr/local/include
SHLIB_LINK  += -L/usr/local/lib -llwgeom
endif

OBJS = src/decoderbufs.o src/proto/pg_logicaldec.pb-c.o

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: proto

proto:
	protoc-c --c_out=src/ proto/pg_logicaldec.proto
