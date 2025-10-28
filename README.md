SSDcache is a small and non-invasive DuckDB extension that adds SSD (disk) caching to its ExternalFileCache (enable_external_file_cache = true by default).  That ExternalFileCache (CachingFileSystem) is specifically geared towards accessing Parquet files in Data Lakes (ducklake, iceberg, delta), and caches data in RAM via the DuckDB Buffer Manager. 

This SSDcache extension significantly enlarges the data volumes that can be quickly cached by also storing data on local disk.

It works by intercepting reads that go through httpfs, s3, hf, azure, r2 and gcp and will write what you read to a local file. Subsequent reads will be served from that.  By default, it will only cache parquet files, read via a Data Lake. For those parquet reads, it is known that the underlying parquet files do not change in the cloud, and therefore can be safely cached.

There the possibility (off by default) to SSDcache much generally, not only parquet files, by specifying regeular expressions on the URIs passed to any external file reader (not only parquet, but also json, csv, ..). Note that this is unsafe. Because SSDcache operates after the ExternalFileCache from DuckDB, it does not know whether DuckDB asks to re-read a parquet file because it got swapped out of RAM, or because DuckDB detected a new modification time (or etag). In the latter case, SSDcache will deliver stale data. Note that this cannot happen in Data Lake Reads. Therefore, when using free-form regexps for SSD it is recommended to set external_file_cache = false;

SSDcache supports a fakes_3://X filesystem which acts like a local filesystem, but adds fake network latencies similar to S3 latencies as observed inside the same region. This is a handy tool for local performance debugging without having to spin up an EC2 instance.

You can configure with: FROM ssdcache_config(directory, max_size_mb, nr_io_threads, regexps="");

You can inspect the configuration by invoking that without parameters.
You can reconfigure an existing cache by changing all parameters except the first (the directory). If you change the directory (where the cached file ranges are stored), then the cache gets cleared.

The regexps parameter contains semicolon-separated regexps that allow more aggressive caching: they will cache any URL that matches one of the regexps.

The current contents of the cache can be queried with FROM ssdcache_stats(); it lists the cache contents in reverse LRU order (hottest ranges first). One possible usage of this TableFunction could be to store the (leading) part of these ranges in a DuckDB table. 

Because, smartcache provides a ssdcache_prefetch(URL, start, size) function that uses massively parallel IO to read and cache these ranges. This parallelism is necessary in cloud instances to get near the maximum network bandwidth, and allows for quick hydration of the smartcache from a previous state.
