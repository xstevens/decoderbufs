decoderbufs
===========

A PostgreSQL logical decoder output plugin to deliver data as Protocol Buffers

# try this out

The easiest way to try out changes in this repo, is to build the [docker-postgres](https://github.com/remerge/docker-postgres) around this addon. In order to achieve this, you have to clone this repo into the `docker-postgres` repo and swap out the `git clone` [in the dockerfile](https://github.com/remerge/docker-postgres/blob/56d05cdd6d4c32dcffa6e0a1408c23e058b5aa14/Dockerfile#L20) for a `COPY decoderbufs decoderbufs`: 
```
RUN git clone --depth 1 https://github.com/remerge/decoderbufs.git && ...
```
=> 
```
COPY decoderbufs decoderbufs
RUN ...
```

You can then build the docker-image (this can take a while, grab a coffee or something...) with `docker build -t custom-postgres .` (or any tag you like) and run it with `docker run -v /some/local/path/for/data/:/var/lib/postgresql/data -p 5432:5432 --rm custom-postgres`.

One easy way of interacting with the database is to use the [rails app](https://github.com/remerge/api). Get a fresh database with `bundle exec rails db:fresh` and seed it with `bundle exec rails db:seed`.

Then you can use the rails console to interact with the database: `bundle exec rails console` `c = Campaign.last; c.description= "134567890" * 10000; c.save; c.name = "jeff"`

After this you can start kafka with `docker run -d --name kafka -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=localhost --env ADVERTISED_PORT=9092 spotify/kafka` and [p2q](https://github.com/remerge/p2q).

You should already get messages in the kafka topic `changes_v2`, which you can read with [rcmd](https://github.com/remerge/recmd) or just with a small go script using [sarama](https://github.com/Shopify/sarama) and our [decoderbuf definition](github.com/remerge/decoderbufs-proto-go):
```go
package main

import (
	"fmt"

	"github.com/Shopify/sarama"
	bufs "github.com/remerge/decoderbufs-proto-go"
)

func main() {
	config := sarama.NewConfig()
	fmt.Println("starting")
	client, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		panic(err)
	}
	fmt.Println("consumer ..")
	consumer, err := client.ConsumePartition("changes_v2", 0, sarama.OffsetNewest)
	if err != nil {
		panic(err)
	}
	fmt.Println("will read")
	counter := 0
loop:
	for {
		select {
		case msg := <-consumer.Messages():
			message := &bufs.RowMessage{}
			err := message.Unmarshal(msg.Value)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Println(message)
			counter++
			fmt.Println("counter now: ", counter)
		case <-consumer.Errors():
			break loop
		}
	}
}
```


You especially want to try out toast cases, which you get by having a value > 1-2kb in one column and updating another column.

# decoderbufs

Version: 0.1.0

**decoderbufs** is a PostgreSQL logical decoder output plugin to deliver data as Protocol Buffers.

**decoderbufs** is released under the MIT license (See LICENSE file).

**Shoutouts:**
- [The PostgreSQL Team](https://postgresql.org) for adding [logical decoding](http://www.postgresql.org/docs/9.4/static/logicaldecoding.html) support. This is a immensely useful feature.
- [Michael Paquier](https://github.com/michaelpq) for his [decoder_raw](https://github.com/michaelpq/pg_plugins/tree/master/decoder_raw)
project and blog posts as a guide to teach myself how to write a PostgreSQL logical decoder output plugin.
- [Martin Kleppmann](https://github.com/ept) for making me aware that PostgreSQL was working on logical decoding.
- [Magnus Edenhill](https://github.com/edenhill) for letting me know that protobuf-c existed and just in general for writing librdkafka.

### Version Compatability
This code is built with the following assumptions.  You may get mixed results if you deviate from these versions.

* [PostgreSQL](http://www.postgresql.org) 9.4+
* [Protocol Buffers](https://developers.google.com/protocol-buffers) 2.6.1
* [protobuf-c](https://github.com/protobuf-c/protobuf-c) 1.1.0
* [PostGIS](http://postgis.net) 2.1.x

### Requirements
* PostgreSQL
* PostGIS
* Protocol Buffers
* protobuf-c

### Building

#### Simplified version
* Simple build can be done using docker
```
 docker build
```

#### Full version

To build you will need to install PostgreSQL (for pg_config) and PostgreSQL server development packages. On Debian
based distributions you can usually do something like this:

    apt-get install -y postgresql postgresql-server-dev-9.4

You will also need to make sure that protobuf-c and it's header files have been installed. See their Github
page for further details.

If you have all of the prerequisites installed you should be able to just:

    make && make install

Once the extension has been installed you just need to enable it and logical replication in postgresql.conf:

    # MODULES
    shared_preload_libraries = 'decoderbufs'

    # REPLICATION
    wal_level = logical             # minimal, archive, hot_standby, or logical (change requires restart)
    max_wal_senders = 8             # max number of walsender processes (change requires restart)
    wal_keep_segments = 4           # in logfile segments, 16MB each; 0 disables
    #wal_sender_timeout = 60s       # in milliseconds; 0 disables
    max_replication_slots = 4       # max number of replication slots (change requires restart)

In addition, permissions will have to be added for the user that connects to the DB to be able to replicate. This can be modified in _pg\_hba.conf_ like so:

    local   replication     <youruser>                          trust
    host    replication     <youruser>  127.0.0.1/32            trust
    host    replication     <youruser>  ::1/128                 trust

And restart PostgreSQL.

### Usage
    -- can use SQL for demo purposes
    select * from pg_create_logical_replication_slot('decoderbufs_demo', 'decoderbufs');

    -- DO SOME TABLE MODIFICATIONS (see below about UPDATE/DELETE)

    -- peek at WAL changes using decoderbufs debug mode for SQL console
    select data from pg_logical_slot_peek_changes('decoderbufs_demo', NULL, NULL, 'debug-mode', '1');
    -- get WAL changes using decoderbufs to update the WAL position
    select data from pg_logical_slot_get_changes('decoderbufs_demo', NULL, NULL, 'debug-mode', '1');

    -- check the WAL position of logical replicators
    select * from pg_replication_slots where slot_type = 'logical';

If you're performing an UPDATE/DELETE on your table and you don't see results for those operations from logical decoding, make sure you have set [REPLICA IDENTITY](http://www.postgresql.org/docs/9.4/static/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) appropriately for your use case.

For consuming the binary format, I have implemented a custom PostgreSQL logical replication client that publishes to Apache Kafka. I'm hoping to clean that up a bit and open source the project.

### Type Mappings

The following table shows how current PostgreSQL type OIDs are mapped to which decoderbuf fields:

| PostgreSQL Type OID | Decoderbuf Field |
|---------------------|---------------|
| BOOLOID             | datum_boolean |
| INT2OID             | datum_int32   |
| INT4OID             | datum_int32   |
| INT8OID             | datum_int64   |
| OIDOID              | datum_int64   |
| FLOAT4OID           | datum_float   |
| FLOAT8OID           | datum_double  |
| NUMERICOID          | datum_double  |
| CHAROID             | datum_string  |
| VARCHAROID          | datum_string  |
| BPCHAROID           | datum_string  |
| TEXTOID             | datum_string  |
| JSONOID             | datum_string  |
| XMLOID              | datum_string  |
| UUIDOID             | datum_string  |
| DATEOID             | datum_string  |
| TIMESTAMPOID        | datum_string  |
| TIMESTAMPTZOID      | datum_string  |
| BYTEAOID            | datum_bytes   |
| POINTOID            | datum_point   |
| PostGIS geometry    | datum_point   |
| PostGIS geography   | datum_point   |

### Support

File bug reports, feature requests and questions using
[GitHub Issues](https://github.com/xstevens/decoderbufs/issues)

### Notes

This approach is the one we wanted when we first started kicking around ideas for how to replicate our Postgres DBs in near-realtime. It should provide much better
resiliency in the face of network outages and unplanned downtime than a push mechanism (like using pg_kafka with a trigger) would.

The PostgreSQL docs are pretty good and are definitely worth a read.

**NOT ALL OID TYPES ARE SUPPORTED CURRENTLY**. I really want to iterate this point. There are lots of OIDs in Postgres. Right now I'm translating the ones that I see used a lot, but it is by no means comprehensive. I hope to update this project to support most if not all types at some point.
