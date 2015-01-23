# presto-streaming

Presto-streaming is a Presto plugin that allows stream processing from other Presto connectors. You can create a stream using CREATE VIEW statement:

```sql
CREATE VIEW stats_hourly AS 
   SELECT count(*) total_count, avg(response_time) response_time
   FROM page_views
   GROUP BY date_trunc('hour', timestamp)
```

Then you can insert data to the stream using INSERT DATA statement:

```sql
INSERT TABLE stats_hourly 
   SELECT count(*) total_count, avg(response_time) response_time
   FROM kafka_consumer_response_time
   GROUP BY date_trunc('hour', timestamp)
```

CREATE_VIEW does not actually execute the query, instead it parses the query, identifies the columns and creates the appropriate accumulators for aggregation columns.
When you execute an INSERT TABLE statement on a view, the query is executed and the plugin merges the result with the result of previous queries.
It's very much like materialized views but instead of refreshing the materialized view, you manually need to send new data-set to the plugin so that it continuously aggregates the data-set.
If you use Hive to store your data, you can continuously aggregate your data using this approach:

```sql
INSERT TABLE stats_hourly 
   SELECT count(*) total_count, avg(response_time) response_time
   FROM page_views
   WHERE timestamp > timestamp '2015-01-01 00:00'
   GROUP BY date_trunc('hour', timestamp)
```
Then you can continuously aggregate your data by executing the query each 30 minutes for previous window, presto-stream will take care of each batches and always returns the fresh aggregation results.

Another clever use case is to use a queue in front of Presto so that you can consume the data in the queue and aggregate your data based on your pre-aggregation queries. Presto already have a plugin for Kafka which is a great distributed commit log that can be used as a queue. If you send your rows to Kafka, you can process the data using Presto and stream the results to presto-streaming plugin. It's very much like the way Storm and Spark streaming done streaming processing so if you need streaming feature in order to aggregate your data, you may use presto-streaming instead of Storm trident or Spark streaming.

In order to perform the aggregation, the data needs to be in-memory so persistence in presto-streaming allows you to backup your data so that when you restart a node which owns a stream, it can recover the data from disk when you perform an INSERT TABLE query.
If you enable persistence support, the table doesn't need to be in-memory all the time but it has to fit in memory of a node because currently a stream lives in a node in Presto cluster.

## TODO
  - Persistence
  - Replication
  - Support windowed operations
  - Support for DISTINCT statement
