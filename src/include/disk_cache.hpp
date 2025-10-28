#pragma once

// Undefine Windows macros BEFORE any includes
#ifdef WIN32
#undef CreateDirectory
#undef MoveFile
#undef RemoveDirectory
#endif

#include "duckdb.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/external_file_cache.hpp"
#include <regex>
#include <iomanip>
#include <thread>
#include <atomic>

// inspired on AnyBlob paper: lowest latency is 20ms, transfer 12MB/s for the first MB, 40MB/s beyond that
#define EstimateS3(nr_bytes) ((nr_bytes < (1 << 20)) ? (20 + ((80 * nr_bytes) >> 20)) : (75 + ((25 * nr_bytes) >> 20)))

namespace duckdb {

// Forward declarations
struct DiskCache;

// Write canceled marker for nr_bytes field
constexpr size_t WRITE_CANCELED = static_cast<size_t>(-1);

// WriteBuffer - shared buffer for async writes
struct WriteBuffer {
	char *buf;       // Buffer containing data to write (owned, will be deleted)
	size_t nr_bytes; // Size to write, or WRITE_CANCELED if canceled

	WriteBuffer() : buf(nullptr), nr_bytes(0) {
	}
	~WriteBuffer() {
		if (buf) {
			delete[] buf;
		}
	}
};

//===----------------------------------------------------------------------===//
// DiskCacheFileRange - represents a cached range with its own disk file
// Each range gets its own file, eliminating the need for DiskCacheFile
//===----------------------------------------------------------------------===//
struct DiskCacheFileRange {
	string file;                                                     // Path to cache file
	idx_t uri_range_start, uri_range_end;                            // Range in remote blob file (uri)
	shared_ptr<WriteBuffer> write_buf;                               // Shared write buffer, nullptr when write complete
	idx_t usage_count = 0, bytes_from_cache = 0, bytes_from_mem = 0; // stats
	DiskCacheFileRange *lru_prev = nullptr, *lru_next = nullptr;     // LRU doubly-linked list

	DiskCacheFileRange(string file_path, idx_t id, idx_t start, idx_t end, shared_ptr<WriteBuffer> write_buffer)
	    : file(std::move(file_path)), uri_range_start(start), uri_range_end(end), write_buf(std::move(write_buffer)) {
	}
};

struct DiskCacheEntry {
	string uri; // full URL of the blob
	// Map of start position to DiskCacheFileRanges (shared for IO thread safety)
	map<idx_t, shared_ptr<DiskCacheFileRange>> ranges;
};

// Statistics structure
struct DiskCacheRangeInfo {
	string protocol;        // e.g., s3
	string uri;             // Blob that we have cached a range of
	string file;            // Disk file where this range is stored in the cache
	idx_t range_start_file; // Offset in cache file where this range starts
	idx_t range_start_uri;  // Start position in blob of this range
	idx_t range_size;       // Size of range (end - start in remote file)
	idx_t usage_count;      // how often it was read from the cache
	idx_t bytes_from_cache; // disk bytes read from CacheFile
	idx_t bytes_from_mem;   // memory bytes read from this cached range
};

// DiskCacheWriteJob - async write job for disk persistence
struct DiskCacheWriteJob {
	shared_ptr<WriteBuffer> write_buf;    // Shared write buffer
	shared_ptr<DiskCacheFileRange> range; // Shared ownership of the range being written
	string uri, key;                      // For error handling and cache invalidation
};

// DiskCacheReadJob - async read job for prefetching
struct DiskCacheReadJob {
	string uri, key;   // Cache uri and derived key of the blob that gets cached
	idx_t range_start; // Start position in file
	idx_t range_size;  // Bytes to read
};

//===----------------------------------------------------------------------===//
// DiskCache - Main cache implementation (merged DiskCacheState + DiskCacheMap + DiskCache)
//===----------------------------------------------------------------------===//

struct DiskCache {
	static constexpr idx_t MAX_IO_THREADS = 256;
	static constexpr idx_t URI_SUFFIX_LEN = 15;

	// Configuration and state
	shared_ptr<DatabaseInstance> db_instance;
	bool disk_cache_initialized = false;
	bool disk_cache_shutting_down = false;
	string path_sep;       // normally "/", but "\" on windows
	string disk_cache_dir; // where we store data temporarily
	idx_t total_cache_capacity = 0;

	// Memory cache for disk-cached files (our own ExternalFileCache instance)
	unique_ptr<ExternalFileCache> blobfile_memcache;

	// Directory management
	mutable std::mutex subdir_mutex;
	std::bitset<4096 + 4096 * 256> subdir_created; // 4096*256 (XXX/YY directories)

	// Cache maps and LRU
	mutable std::mutex disk_cache_mutex; // Protects cache, LRU lists, sizes
	unique_ptr<unordered_map<string, unique_ptr<DiskCacheEntry>>> key_cache;
	DiskCacheFileRange *lru_head = nullptr, *lru_tail = nullptr; // Range-based LRU
	idx_t current_cache_size = 0, nr_ranges = 0, current_file_id = 10000000;

	// Cached regex patterns for file filtering
	mutable std::mutex regex_mutex;
	vector<std::regex> cached_regexps;

	// Multi-threaded background I/O system
	std::array<std::thread, MAX_IO_THREADS> io_threads;
	std::array<std::queue<DiskCacheWriteJob>, MAX_IO_THREADS> write_queues;
	std::array<std::queue<DiskCacheReadJob>, MAX_IO_THREADS> read_queues;
	std::array<std::mutex, MAX_IO_THREADS> io_mutexes;
	std::array<std::condition_variable, MAX_IO_THREADS> io_cvs;
	std::atomic<bool> shutdown_io_threads;
	std::atomic<idx_t> read_job_counter;
	idx_t nr_io_threads;

	// Constructor/Destructor
	explicit DiskCache(DatabaseInstance *db_instance_p = nullptr)
	    : key_cache(make_uniq<unordered_map<string, unique_ptr<DiskCacheEntry>>>()), shutdown_io_threads(false),
	      read_job_counter(0), nr_io_threads(1) {
		if (db_instance_p) {
			db_instance = db_instance_p->shared_from_this();
		}
	}
	~DiskCache() {
		disk_cache_shutting_down = true;
		StopIOThreads();
	}

	// Logging methods
	void LogDebug(const string &message) const {
		if (db_instance && !disk_cache_shutting_down) {
			DUCKDB_LOG_DEBUG(*db_instance, "[DiskCache] %s", message.c_str());
		}
	}
	void LogError(const string &message) const {
		if (db_instance && !disk_cache_shutting_down) {
			DUCKDB_LOG_ERROR(*db_instance, "[DiskCache] %s", message.c_str());
		}
	}

	// File system helpers
	FileSystem &GetFileSystem() const {
		return FileSystem::GetFileSystem(*db_instance);
	}

	// Cache key and file path generation
	string GenCacheKey(const string &uri) const {
		const idx_t len = uri.length();
		const idx_t suffix = (len > URI_SUFFIX_LEN) ? len - URI_SUFFIX_LEN : 0;
		const idx_t slash = uri.find_last_of(path_sep);
		const idx_t protocol = uri.find("://");
		hash_t hash_value = Hash(string_t(uri.c_str(), static_cast<uint32_t>(len)));
		std::stringstream hex_stream;
		hex_stream << std::hex << std::uppercase << std::setfill('0') << std::setw(16) << hash_value;
		return hex_stream.str() + "_" + uri.substr(std::max<idx_t>((slash != string::npos) * (slash + 1), suffix)) +
		       "_" + ((protocol != string::npos) ? StringUtil::Lower(uri.substr(0, protocol)) : "unknown");
	}

	string GenCacheFilePath(idx_t file_id, const string &key, idx_t range_start = 0) const {
		std::ostringstream oss;
		string xxx = key.substr(0, 3);
		string yy = key.substr(3, 2);
		oss << range_start << "_" << file_id;
		return disk_cache_dir + xxx + path_sep + yy + path_sep + key.substr(5, 11) + oss.str() + key.substr(16);
	}

	// Directory management
	void EnsureDirectoryExists(const string &key);
	bool CleanCacheDir();
	bool InitCacheDir();

	// Memory cache helpers
	void InsertRangeIntoMemcache(const string &file_path, idx_t file_range_start, BufferHandle &handle, idx_t len);
	bool TryReadFromMemcache(const string &file_path, idx_t file_range_start, void *buffer, idx_t &len);
	bool AllocateInMemCache(BufferHandle &handle, idx_t length) {
		try {
			handle = blobfile_memcache->GetBufferManager().Allocate(MemoryTag::EXTERNAL_FILE_CACHE, length);
			return true;
		} catch (const std::exception &e) {
			LogError("AllocateInMemCache: failed for '" + to_string(length) + " bytes: " + string(e.what()));
			return false;
		}
	}

	// Cache map operations
	void Clear() {
		key_cache->clear();
		lru_head = lru_tail = nullptr;
		current_cache_size = nr_ranges = 0;
	}

	DiskCacheEntry *FindFile(const string &key, const string &uri) {
		auto it = key_cache->find(key);
		return (it != key_cache->end() && it->second->uri == uri) ? it->second.get() : nullptr;
	}

	DiskCacheEntry *UpsertFile(const string &key, const string &uri) {
		DiskCacheEntry *cache_entry = nullptr;
		auto it = key_cache->find(key);
		if (it == key_cache->end()) {
			auto new_entry = make_uniq<DiskCacheEntry>();
			new_entry->uri = uri;
			LogDebug("Insert key '" + key + "'");
			cache_entry = new_entry.get();
			(*key_cache)[key] = std::move(new_entry);
		} else if (it->second->uri == uri) {
			cache_entry = it->second.get();
		}
		return cache_entry;
	}

	void EvictFile(const string &uri) {
		if (!disk_cache_initialized) {
			return;
		}
		std::lock_guard<std::mutex> lock(disk_cache_mutex);
		string key = GenCacheKey(uri);
		EvictCache(uri, key);
	}

	void EvictCache(const string &uri, const string &key);

	// LRU management
	void TouchLRU(DiskCacheFileRange *range) {
		if (range != lru_head) {
			RemoveFromLRU(range);
			AddToLRUFront(range);
		}
	}
	void RemoveFromLRU(DiskCacheFileRange *range) {
		if (range->lru_prev) {
			range->lru_prev->lru_next = range->lru_next;
		} else {
			lru_head = range->lru_next;
		}
		if (range->lru_next) {
			range->lru_next->lru_prev = range->lru_prev;
		} else {
			lru_tail = range->lru_prev;
		}
		range->lru_prev = range->lru_next = nullptr;
	}

	void AddToLRUFront(DiskCacheFileRange *range) {
		range->lru_next = lru_head;
		range->lru_prev = nullptr;
		if (lru_head) {
			lru_head->lru_prev = range;
		}
		lru_head = range;
		if (!lru_tail) {
			lru_tail = range;
		}
	}

	// File operations
	bool EvictToCapacity(idx_t required_space);
	unique_ptr<FileHandle> TryOpenCacheFile(const string &file_path);
	bool WriteToCacheFile(const string &file_path, const void *buffer, idx_t length);
	bool ReadFromCacheFile(const string &file_path, void *buffer, idx_t &length, idx_t &out_bytes_from_mem);
	bool DeleteCacheFile(const string &file_path);
	vector<DiskCacheRangeInfo> GetStatistics() const;

	// Thread management
	void MainIOThreadLoop(idx_t thread_id);
	void ProcessWriteJob(DiskCacheWriteJob &job);
	void ProcessReadJob(DiskCacheReadJob &job);
	void QueueIOWrite(DiskCacheWriteJob &job, idx_t partition);
	void QueueIORead(DiskCacheReadJob &job);
	void StartIOThreads(idx_t thread_count);
	void StopIOThreads();

	// Core cache operations
	void InsertCache(const string &key, const string &uri, idx_t pos, idx_t len, void *buf);
	idx_t ReadFromCache(const string &key, const string &uri, idx_t pos, idx_t &len, void *buf);

	// Configuration and caching policy
	void ConfigureCache(const string &directory, idx_t max_size_bytes, idx_t writer_threads);
	bool CacheUnsafely(const string &uri) const;
	void UpdateRegexPatterns(const string &regex_patterns_str);
};

} // namespace duckdb
