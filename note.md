Mermaid `architecture-beta` has only five built-in icons: `cloud`, `database`, `disk`, `internet`, and `server`. [Mermaid documentation](https://mermaid.js.org/syntax/architecture.html)

For portable Markdown rendering, I’d use:

```mermaid
group streaming(internet)[Streaming Path]
    service collector(server)[Python Collector] in streaming
    service kafka(database)[Kafka Topics] in streaming
    service streamJob(server)[PySpark Streaming] in streaming
    service candleTopic(database)[Kafka Candle Topic] in streaming
```

`internet` better communicates a streaming/transport path, while `database` distinguishes Kafka topics as durable data stores.

If your Mermaid renderer registers Iconify packs, a richer version would be:

```mermaid
group streaming(mdi:chart-sankey)[Streaming Path]
    service collector(logos:python)[Python Collector] in streaming
    service kafka(mdi:apache-kafka)[Kafka Topics] in streaming
    service streamJob(logos:apache-spark)[PySpark Streaming] in streaming
    service candleTopic(lucide:chart-candlestick)[Kafka Candle Topic] in streaming
```

Those custom icons are available through Iconify: [Python](https://icon-sets.iconify.design/logos/python/), [Kafka](https://icon-sets.iconify.design/mdi/apache-kafka/), [Spark](https://icon-sets.iconify.design/logos/), and [candlestick chart](https://icon-sets.iconify.design/lucide/chart-candlestick/). However, unregistered packs render as question marks, so the built-in version is safest for a repository Markdown file.