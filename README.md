This tool executes various usage patterns of CockroachDB as used in production at Mesosphere.

It is intended to discover and justify the addition of various indexes and query optimisations.

# Requirements

In addition to this repository you need
- docker (to run cockroachdb)
- go (to run the load tests)

# Running

docker run --net=host cockroachdb/cockroach:v1.0.2 start --insecure --http-port=0 --port=12340
docker run --net=host cockroachdb/cockroach:v1.0.2 start --insecure --http-port=0 --port=12341 --join=localhost:12340
docker run --net=host cockroachdb/cockroach:v1.0.2 start --insecure --http-port=0 --port=12342 --join=localhost:12340
docker run --net=host cockroachdb/cockroach:v1.0.2 start --insecure --http-port=0 --port=12343 --join=localhost:12340
docker run --net=host cockroachdb/cockroach:v1.0.2 start --insecure --http-port=0 --port=12344 --join=localhost:12340

go get -v github.com/gpaul/cockroachload/load
./bin/load -addr=localhost:12340 -verbose
