#### Required
* [Docker](https://docs.docker.com/engine/install/)
* [CapNProto](https://capnproto.org/install.html)
* [Kafka](https://kafka.apache.org/quickstart)
* [clickhouse-client](https://clickhouse.com/docs/en/getting-started/install/)

![](anonymizer.png)

#### Task specification
Imagine a logging pipeline of HTTP records, data collected from edge
servers and sent to an Apache Kafka topic called `http_log` as messages. Your task consumes
those messages, performs Cap'N Proto decoding, transforms the data as needed, and inserts them
to a ClickHouse table. The Cap'N Proto schema is
[http_log.capnp](http_log.capnp).

Because of the GDPR regulations you have to anonymize the client IP. For
each record change `remoteAddr`'s last octet to X before sending it over to
ClickHouse (e.g. 1.2.3.`4` -> 1.2.3.`X`).

* Each record must be stored to ClickHouse, even in the event of network or server error. Make sure
that you handle those appropriately.
* Your application should communicate with the ClickHouse server only through the proxy which has
rate limiting for a 1 request per minute limit.
* If there are any limitation about your application write down what they are, and how would you solve them in the future.
  For example
  * What is the latency of the data?
  * Is there any scenario when you will start losing data?
  * Is it possible that there will be a stored duplicate record?
* You can implement the task in any of those languages:
  * Go
  * C/C++
  * Java
  * Rust

### SQL / ClickHouse part

Load those data into ClickHouse, using a new table called `http_log` with the following columns.

```
  timestamp DateTime
  resource_id UInt64
  bytes_sent UInt64
  request_time_milli UInt64
  response_status UInt16
  cache_status LowCardinality(String)
  method LowCardinality(String)
  remote_addr String
  url String
```

Provide a table with ready made totals of served traffic for any combination of resource ID, HTTP status,
cache status and IP address. The totals need to be queried efficiently, in seconds at best, for on-demand
rendering of traffic charts in a front-end such as Grafana.

Characterize the aggregated select query time to show the table architecture is fit for purpose.
Provide an estimate of disk space required given
 1) average incoming message rate
 2) retention of the aggregated data

### Testing environment

You can use included [docker-compose.yml](docker-compose.yml) to setup a local
development environment with every service that you will need for this task.

```bash
$ docker-compose up -d
[+] Running 10/10
 ⠿ Network data-engineering-task_default  Created   0.0s
 ⠿ Container zookeeper                    Started   0.9s
 ⠿ Container prometheus                   Started   0.8s
 ⠿ Container clickhouse                   Started   0.9s
 ⠿ Container ch-proxy                     Started   1.2s
 ⠿ Container broker                       Started   1.3s
 ⠿ Container grafana                      Started   1.2s
 ⠿ Container log-producer                 Started   1.7s
 ⠿ Container kafka-ui                     Started   1.9s
 ⠿ Container jmx-kafka                    Started   1.8s
 ```

After running `docker-compose up -d`, Kafka is available on local port 9092.
You can test it with

```bash
$ kafka-console-consumer --bootstrap-server localhost:9092 \
                         --topic http_log \
                         --group data-engineering-task-reader
```

For testing, a service called `http-log-kafka-producer` is provided, started alongside Kafka.
It produces synthetic Cap'n Proto-encoded records. You can adjust its rate of producing via the
`KAFKA_PRODUCER_DELAY_MS` environment variable inside [docker-compose.yml](./docker-compose.yml)

ClickHouse should be accessed through a proxy running on HTTP port 8124.

For convenience, [Kafka-UI](localhost:4000/) and [Grafana](localhost:3000/) are running
(user `admin` and password `kafka`) that might be useful if you run into any trouble
with Kafka.

## Solution
### Exploration and thinking about the problem
From the task description, the goal of the application is to buffer Cap'n Proto–serialized messages coming from Kafka and ingest them into ClickHouse through a rate-limited proxy. Since the proxy’s rate limiting prevents uploading data arbitrarily, the goal should be to send as much data as possible with each request. This approach (conveniently) aligns with [insert-in-large-batch-sizes](https://clickhouse.com/docs/guides/inserting-data#insert-in-large-batch-sizes).

However, since the payload size is [limited by nginx to 1MB by default](https://nginx.org/en/docs/http/ngx_http_core_module.html#client_max_body_size) (unless configured otherwise), there is a ceiling to how much data can flow through equal to 1MB / 60s (maximum size of payload / rate limiting). If `http-log-kafka-producer` generates more than that, the messages will accumulate in the memory of application. Therefore, if the buffer is full, the application must either offload data to external storage or discard messages.

The proxy limits connections to ClickHouse to its HTTP interface, which prevents the use of the native TCP connector. According to the documentation, Clickhouse [supports two official Go clients](https://clickhouse.com/docs/integrations/go#clickhouse-go-client), of which, only the high-level `clickhouse-go` supports HTTP (experimental), which [only allows database/sql format](https://clickhouse.com/docs/integrations/go#choosing-a-client). From the [clickhouse SQL reference](https://clickhouse.com/docs/sql-reference), it doesn't seem like there are efficient data loading capabilities beyond sending large `INSERT ... VALUES (...)` statements, which is inefficient. For this reason, manually sending requests with `http.Client` (Go) seems to be the correct approach.

Because of anonymization of the IP address, deserialization inside the application is necessary. Otherwise it would be optimal to send the Cap'n Proto data as is because [Clickhouse supports the Cap'N proto format](https://clickhouse.com/docs/interfaces/formats/CapnProto), assuming we provided the schema. Since the data is already deserialized in the app, it may be more efficient to send them as `application/octet-stream` in [RowBinary Clickhouse format](https://clickhouse.com/docs/interfaces/formats/RowBinary) which may be more efficient. The disadvantage of this approach is that if there was a change to the schema, the application code needs to change. My application was originally designed using this approach, but it may have been expected to serialize back into Cap'N Proto.

To interact with Kafka, the [confluent-kafka-go library](github.com/confluentinc/confluent-kafka-go) can be used.

### The application
Refer to source code and doc comments. 

High-level description: A goroutine running `KafkaLogConsumer` consumes `http_log` Kafka topic and pushes deserialized messages to `LogQueue`, which are promptly consumed by `Anonymizer` goroutine and subsequently appended to `stagingBatch` byte buffer in RowBinary format. Once the buffer is full or when the proxy is ready to receive another batch and the `batchQueue` is empty, the `stagingBatch` is appended to `batchQueue`. Each 60 + 3 seconds after last successful request to Clickhouse, a batch is taken from `batchQueue` and sent to Clickhouse. On server / network error, the operation is repeated until successful. Once successful, the largest offset in the batch is commited to Kafka.

#### How to run
```
docker-compose up
clickhouse-client --host localhost --port 9000 --multiquery < etc/clickhouse/clickhouse_schema.sql
go run ./src
```

#### Limitations & Area for improvements
The main drawback of the current application is that messages are dropped once the batchQueue buffer is full. As stated above, there should be a fallback mechanism that offloads the data outside of the application's memory space. A simple approach is to write excess data to local disk and reload it when the workload decreases. However, this is limited by disk capacity. A more robust approach would use elastic cloud storage (e.g., AWS S3) to guarantee that logs are never lost. Since this would require connecting to a cloud service, I will not be implementing this.

Duplicates should not happen under normal conditions. Kafka messages are only commited once they are successfully delivered to Clickhouse. However, if the application crashed for whatever reason after `flushToClickhouse()` and before `commitOffset()`, and it was promptly restarted (perhaps by an automatic process), the delivered but uncommited messages would be buffered and delivered to Clickhouse again. If this was a concern, one solution could be using `ReplacingMergeTree` in Clickhouse.

If the `http-log-kafka-product` generates less than 1 MB / 60 s, the latency is dictated by rate-limited proxy flush window. In best case, message arrives just before the flush to Clickhouse happens and there are no network / server errors, and the latency will be equivalent to network latency. In worst case, message arrives just after the flush to Clickhouse happened, and the message is buffered for the next flush, which happens in `proxyRateLimit` + `proxyTimeoutSlack` + any delay caused by retries due to server / network errors. If `http-log-kafka-product` generates more than 1 MB / 60 s, the latency can be unbounded, since the batches will accumulate in the queue, capped by size of `batchQueue`. In the current setup, this is difficult to mitigate. It could be improved by increasing throughput of the application by increasing rate limit or max payload size, or allowing the application to access Clickhouse directly. A clever solution may be to distribute batches across several instances of the application.

### Clickhouse part
#### Schema
The `http_log` table uses MergeTree() for throughput and simplicity, though `ReplacingMergeTree()` could be used if eventual deduplication is required. Partitioning by day makes it convenient to extract or retire older logs in line with [choose a low cardinality partitioning key](https://clickhouse.com/docs/best-practices/choosing-a-partitioning-key#choose-a-low-cardinality-partitioning-key)
```sql
CREATE TABLE http_log
(
    `timestamp` DateTime,
    `resource_id` UInt64,
    `bytes_sent` UInt64,
    `request_time_milli` UInt64,
    `response_status` UInt16,
    `cache_status` LowCardinality(String),
    `method` LowCardinality(String),
    `remote_addr` String,
    `url` String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, resource_id);
```

[Cost of computation can be shifted from query time to insert time](https://clickhouse.com/docs/materialized-view/incremental-materialized-view) with incremental materialized views, which enables the aggregation query times specified in the tasks.

```sql
CREATE TABLE http_log_agg
(
    `timestamp` DateTime,
    `resource_id` UInt64,
    `response_status` UInt16,
    `cache_status` LowCardinality(String),
    `remote_addr` String,
    `total_bytes` UInt64,
    `request_count` UInt64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, resource_id, response_status, cache_status, remote_addr);
```

```sql
CREATE MATERIALIZED VIEW http_log_matview
TO http_log_agg
AS
SELECT
    toStartOfMinute(timestamp) AS timestamp,
    resource_id,
    response_status,
    cache_status,
    remote_addr,
    sum(bytes_sent) AS total_bytes,
    count() AS request_count
FROM http_log
GROUP BY timestamp, resource_id, response_status, cache_status, remote_addr;
```

Example Query 1: Traffic by timestamp

```sql
SELECT
    timestamp,
    sum(total_bytes) AS total_bytes,
    sum(request_count) AS requests
FROM http_log_agg
GROUP BY timestamp
ORDER BY timestamp;
```

Example Query 2: Top 10 resources based on total_bytes

```sql
SELECT
    resource_id,
    sum(total_bytes) AS total_bytes
FROM http_log_agg
WHERE timestamp >= now() - INTERVAL 1 HOUR
GROUP BY resource_id
ORDER BY total_bytes DESC
LIMIT 10;
```

Example Query 3: Requests caching status by timestamp
```sql
SELECT
    timestamp,
    cache_status,
    sum(request_count) AS requests
FROM http_log_agg
WHERE timestamp >= now() - INTERVAL 1 DAY
GROUP BY timestamp, cache_status
ORDER BY timestamp ASC;
```

#### Benchmark
It can be shown that the table architecture is fit for purpose by empirically measuring query time of the above queries on different table sizes. The anonymizer program was ran in order to insert roughly 10k and 100k rows. Additionally, a random generator was used to generate 1M, 10M and 100M rows. Single measurements were taken on a office laptop with i7-1185G7 in Debian WSL2.

Example of a measurement:
```
SELECT
    resource_id,
    sum(total_bytes) AS total_bytes
FROM http_log_agg
WHERE timestamp >= (now() - toIntervalHour(1))
GROUP BY resource_id
ORDER BY total_bytes DESC
LIMIT 10

Query id: 11b3995c-5814-4cb2-8cbd-f5cd233db113

    ┌─resource_id─┬─total_bytes─┐
 1. │   774583802 │     3901948 │
 2. │   238854264 │     3801414 │
 3. │   621192785 │     3542583 │
 4. │   457745089 │     2778794 │
 5. │   636451554 │     2721865 │
 6. │   529483128 │     2097127 │
 7. │   511357887 │     2097097 │
 8. │   165458523 │     2097085 │
 9. │   720510926 │     2097062 │
10. │   220216470 │     2097022 │
    └─────────────┴─────────────┘

10 rows in set. Elapsed: 0.030 sec. Processed 145.25 thousand rows, 2.91 MB (4.80 million rows/s., 96.02 MB/s.)
```

Results:
```
| Samples | Query 1 Time (s) | Query 2 Time (s) | Query 3 Time (s) |
|---------|------------------|------------------|------------------|
| 10k     | 0.005            | 0.007            | 0.014            |
| 100k    | 0.009            | 0.022            | 0.032            |
| 1M      | 0.026            | 0.028            | 0.018            |
| 10M     | 0.041            | 0.062            | 0.109            |
| 100M    | 0.408            | 0.247            | 0.856            |
```

Sample generator:
```sql
insert into http_log SELECT
    now() - (rand() % 86400) AS timestamp,
    intDiv(rand64(), 1000) % 1000000000 AS resource_id,
    rand64() % 5000000 AS bytes_sent,
    rand64() % 50000 AS request_time_milli,
    [200, 301, 400, 403, 404, 500, 502, 504][rand() % 11] AS response_status,
    ['HIT','MISS','STALE','BYPASS','REVALIDATED','UPDATING','SCARCE'][rand() % 7] AS cache_status,
    ['GET','HEAD'][rand() % 3] AS method,
    concat(
        toString(rand() % 256), '.',
        toString(rand() % 256), '.',
        toString(rand() % 256), '.',
        toString(rand() % 256)
    ) AS remote_addr,
    [
        concat('/cdn/js/static/data/', substring(MD5(toString(rand())), 1, 20), '.jpeg'),
        concat('/media/content/data/', substring(MD5(toString(rand())), 1, 20), '.m3u8'),
        concat('/vod/', substring(MD5(toString(rand())), 1, 15), '.json'),
        concat('/content/files/', substring(MD5(toString(rand())), 1, 15), '.js'),
        concat('/imgs/fetch/media/static/', substring(MD5(toString(rand())), 1, 25), '.mpd'),
        concat('/games/static/', substring(MD5(toString(rand())), 1, 10), '.png'),
        concat('/video/js/', substring(MD5(toString(rand())), 1, 15)),
        concat('/stream/content/', substring(MD5(toString(rand())), 1, 25), '.jpg'),
        concat('/images/preview/', substring(MD5(toString(rand())), 1, 20), '.jpg')
    ][rand() % 9] AS url
FROM numbers(100000000);
```

#### Disk size estimate
Since Clickhouse heavily [relies on compression](https://clickhouse.com/docs/data-compression/compression-in-clickhouse), it may be difficult to estimate size of a record once it is in Clickhouse. We can use query from this [section of documentation](https://clickhouse.com/docs/data-compression/compression-in-clickhouse#choose-the-right-data-type-to-optimize-compression) to get an average size of a row generated by `http-log-kafka-producer` in the `http_log` table.

```
SELECT formatReadableSize(sum(c.data_compressed_bytes) / sum(p.rows)) AS avg_compressed_per_row
FROM system.columns AS c
INNER JOIN system.parts AS p
    ON c.table = p.table
   AND c.database = p.database
WHERE c.table = 'http_log'
  AND c.database = currentDatabase()
  AND active

   ┌─avg_compressed_per_row─┐
1. │ 44.50 B                │
   └────────────────────────┘
```

Given that a single log takes on average 44.5 B to be stored, we can estimate the required disk size based on the following formula:

$44.5 * (1000 / `KAFKA_PRODUCER_DELAY_MS`) * retention$

For example, for `KAFKA_PRODUCER_DELAY_MS` = 5000 and retention 30 days (2592000 seconds), the required disk size would be

$44.5 * (1000 / 5000) * 2592000 ~= 23MB$

For `KAFKA_PRODUCER_DELAY_MS` = 10

$44.5 * (1000 / 10) * 2592000 ~= 11.534 GB$

### Time Spent on the task
Roughly ~2-3 weeks of work, mostly spent by
- Tour of Go (some familiarity)
- Going through Kafka & Clickhouse documentation (no prior experience)
- Prototyping, refactoring, writing this report