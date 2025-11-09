

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