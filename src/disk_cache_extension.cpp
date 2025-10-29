#define DUCKDB_EXTENSION_MAIN

#include "disk_cache_extension.hpp"
#include "disk_cache_fs_wrapper.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "duckdb/storage/external_file_cache.hpp"

namespace duckdb {

//===----------------------------------------------------------------------===//
// Table Functions
//===----------------------------------------------------------------------===//

// Bind data for disk_cache_config function
struct DiskCacheConfigBindData : public FunctionData {
	string directory;
	idx_t max_size_mb;
	idx_t nr_io_threads;
	string regex_patterns; // New regex patterns parameter
	bool query_only;       // True if no parameters provided - just query current values

	DiskCacheConfigBindData(string dir, idx_t size, idx_t threads, string regexps = "", bool query = false)
	    : directory(std::move(dir)), max_size_mb(size), nr_io_threads(threads), regex_patterns(std::move(regexps)),
	      query_only(query) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DiskCacheConfigBindData>(directory, max_size_mb, nr_io_threads, regex_patterns, query_only);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DiskCacheConfigBindData>();
		return directory == other.directory && max_size_mb == other.max_size_mb &&
		       nr_io_threads == other.nr_io_threads && regex_patterns == other.regex_patterns &&
		       query_only == other.query_only;
	}
};

// Global state for both table functions
struct DiskCacheGlobalState : public GlobalTableFunctionState {
	idx_t tuples_processed = 0;
	vector<DiskCacheRangeInfo> stats;

	idx_t MaxThreads() const override {
		return 1; // Single threaded for simplicity
	}
};

void default_cache_sizes(DatabaseInstance &db, idx_t &max_size_mb, idx_t &nr_io_threads) {
	max_size_mb = db.NumberOfThreads() * 4096; // 4GB * threads
	nr_io_threads = std::min<idx_t>(256, db.NumberOfThreads());
}

// Bind function for disk_cache_config
static unique_ptr<FunctionData> DiskCacheConfigBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns useful cache statistics
	return_types.push_back(LogicalType::VARCHAR); // cache_path
	return_types.push_back(LogicalType::BIGINT);  // max_size_bytes
	return_types.push_back(LogicalType::BIGINT);  // current_size_bytes
	return_types.push_back(LogicalType::BIGINT);  // io_threads
	return_types.push_back(LogicalType::BOOLEAN); // success

	names.push_back("cache_path");
	names.push_back("max_size_bytes");
	names.push_back("current_size_bytes");
	names.push_back("nr_io_threads");
	names.push_back("success");

	bool query_only = false;         // Default to configuration mode
	string directory = ".diskcache"; // Default
	string regex_patterns = "";      // Default empty (conservative mode)
	idx_t max_size_mb, nr_io_threads;
	default_cache_sizes(*context.db, max_size_mb, nr_io_threads);

	// Check if this is a query-only call (no parameters)
	if (input.inputs.size() == 0) {
		query_only = true;
	}
	// Parse arguments if provided (max_size_mb, directory, nr_io_threads, regex_patterns)
	if (input.inputs.size() >= 1) {
		if (input.inputs[0].IsNull()) {
			throw BinderException("disk_cache_config: max_size_mb cannot be NULL");
		}
		auto size_val = input.inputs[0];
		if (size_val.type().id() != LogicalTypeId::BIGINT && size_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("disk_cache_config: max_size_mb must be an integer");
		}
		max_size_mb = size_val.GetValue<idx_t>();
	}
	if (input.inputs.size() >= 2) {
		if (input.inputs[1].IsNull()) {
			throw BinderException("disk_cache_config: directory cannot be NULL");
		}
		directory = StringValue::Get(input.inputs[1]);
	}
	if (input.inputs.size() >= 3) {
		if (input.inputs[2].IsNull()) {
			throw BinderException("disk_cache_config: nr_io_threads cannot be NULL");
		}
		auto threads_val = input.inputs[2];
		if (threads_val.type().id() != LogicalTypeId::BIGINT && threads_val.type().id() != LogicalTypeId::INTEGER) {
			throw BinderException("disk_cache_config: nr_io_threads must be an integer");
		}
		nr_io_threads = threads_val.GetValue<idx_t>();
		if (nr_io_threads <= 0) {
			throw BinderException("disk_cache_config: nr_io_threads must be positive");
		}
		if (nr_io_threads > 256) {
			throw BinderException("disk_cache_config: nr_io_threads cannot exceed 256");
		}
	}
	if (input.inputs.size() >= 4) {
		if (input.inputs[3].IsNull()) {
			throw BinderException("disk_cache_config: regex_patterns cannot be NULL");
		}
		auto patterns_val = input.inputs[3];
		if (patterns_val.type().id() != LogicalTypeId::VARCHAR) {
			throw BinderException("disk_cache_config: regex_patterns must be a string");
		}
		regex_patterns = StringValue::Get(patterns_val);
	}
	return make_uniq<DiskCacheConfigBindData>(std::move(directory), max_size_mb, nr_io_threads,
	                                          std::move(regex_patterns), query_only);
}

// Init function for disk_cache_config global state
static unique_ptr<GlobalTableFunctionState> DiskCacheConfigInitGlobal(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return make_uniq<DiskCacheGlobalState>();
}

// Init function for disk_cache_stats global state (same as config)
static unique_ptr<GlobalTableFunctionState> DiskCacheStatsInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	return make_uniq<DiskCacheGlobalState>();
}

// Bind function for disk_cache_stats
static unique_ptr<FunctionData> DiskCacheStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// Setup return schema - returns cache statistics with 8 columns
	return_types.push_back(LogicalType::VARCHAR); // protocol
	return_types.push_back(LogicalType::VARCHAR); // uri
	return_types.push_back(LogicalType::VARCHAR); // file
	return_types.push_back(LogicalType::BIGINT);  // range_start_uri
	return_types.push_back(LogicalType::BIGINT);  // range_size
	return_types.push_back(LogicalType::BIGINT);  // usage_count
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_cache
	return_types.push_back(LogicalType::BIGINT);  // bytes_from_mem

	names.push_back("protocol");
	names.push_back("uri");
	names.push_back("file");
	names.push_back("range_start_uri");
	names.push_back("range_size");
	names.push_back("usage_count");
	names.push_back("bytes_from_cache");
	names.push_back("bytes_from_mem");

	return nullptr; // No bind data needed for stats function
}

// disk_cache_config(directory, max_size_mb, nr_io_threads) - Configure the blob cache
static void DiskCacheConfigFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<DiskCacheGlobalState>();

	// Return nothing if we've already processed our single tuple
	if (global_state.tuples_processed >= 1) {
		output.SetCardinality(0);
		return;
	}
	DUCKDB_LOG_DEBUG(*context.db, "[DiskCache] disk_cache_config called");

	// Process the single configuration tuple
	auto shared_cache = GetOrCreateDiskCache(*context.db);
	bool success = false;
	string cache_path = "";
	idx_t max_size_bytes = 0;
	idx_t current_size_bytes = 0;
	idx_t writer_threads = 0;

	if (data_p.bind_data && shared_cache) {
		auto &bind_data = data_p.bind_data->Cast<DiskCacheConfigBindData>();

		if (bind_data.query_only) {
			// Query-only mode - just return current values without changing anything
			DUCKDB_LOG_DEBUG(*context.db, "[DiskCache] Querying cache configuration");
			success = true; // Query always succeeds
		} else {
			// Configuration mode - actually configure the cache
			DUCKDB_LOG_DEBUG(*context.db,
			                 "[DiskCache] Configuring cache: directory='%s', max_size=%zu MB, nr_io_threads=%zu, "
			                 "regex_patterns='%s'",
			                 bind_data.directory.c_str(), bind_data.max_size_mb, bind_data.nr_io_threads,
			                 bind_data.regex_patterns.c_str());

			// Configure cache first (including small_range_threshold)
			shared_cache->ConfigureCache(bind_data.directory, bind_data.max_size_mb * 1024 * 1024,
			                             bind_data.nr_io_threads);

			// Update regex patterns and purge non-qualifying cache entries
			shared_cache->UpdateRegexPatterns(bind_data.regex_patterns);
			success = true;
			// Now that cache is configured, wrap any existing filesystems
			WrapExistingFilesystems(*context.db);
		}
	}
	// Get current cache statistics (works whether configuration succeeded or not)
	if (shared_cache && shared_cache->disk_cache_initialized) {
		cache_path = shared_cache->disk_cache_dir;
		max_size_bytes = shared_cache->total_cache_capacity;
		current_size_bytes = shared_cache->current_cache_size;
		writer_threads = shared_cache->nr_io_threads;
	}
	// Return the statistics tuple
	output.SetCardinality(1);
	output.data[0].SetValue(0, Value(cache_path));                 // cache_path
	output.data[1].SetValue(0, Value::BIGINT(max_size_bytes));     // max_size_bytes
	output.data[2].SetValue(0, Value::BIGINT(current_size_bytes)); // current_size_bytes
	output.data[3].SetValue(0, Value::BIGINT(writer_threads));     // nr_io_threads
	output.data[4].SetValue(0, Value::BOOLEAN(success));           // success
	global_state.tuples_processed = 1;
}

// disk_cache_stats() - Return cache statistics in LRU order with chunking
static void DiskCacheStatsFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<DiskCacheGlobalState>();
	// Load data on first call
	if (global_state.tuples_processed == 0) {
		auto cache = GetOrCreateDiskCache(*context.db);
		global_state.stats = cache->GetStatistics();
	}
	auto &stats = global_state.stats;
	idx_t offset = global_state.tuples_processed;

	idx_t chunk_size = MinValue<idx_t>(STANDARD_VECTOR_SIZE, stats.size() - offset);
	output.SetCardinality(chunk_size);

	for (idx_t i = 0; i < chunk_size; i++) {
		const auto &info = stats[offset + i];
		output.data[0].SetValue(i, Value(info.protocol));
		output.data[1].SetValue(i, Value(info.uri));
		output.data[2].SetValue(i, Value(info.file));
		output.data[3].SetValue(i, Value::BIGINT(info.range_start_uri));
		output.data[4].SetValue(i, Value::BIGINT(info.range_size));
		output.data[5].SetValue(i, Value::BIGINT(info.usage_count));
		output.data[6].SetValue(i, Value::BIGINT(info.bytes_from_cache));
		output.data[7].SetValue(i, Value::BIGINT(info.bytes_from_mem));
	}
	global_state.tuples_processed += chunk_size;
}

//===----------------------------------------------------------------------===//
// disk_cache_prefetch - Table function to prefetch ranges into cache
//===----------------------------------------------------------------------===//

struct PrefetchRange {
	string uri;
	string key;
	idx_t lru_rank;      // LRU rank for ordering
	idx_t start, end;    // range
	idx_t original_size; // Sum of original range sizes
};

struct DiskCachePrefetchGlobalState : public GlobalTableFunctionState {
	std::mutex mutex;
	vector<PrefetchRange> ranges;
	shared_ptr<DiskCache> cache;

	explicit DiskCachePrefetchGlobalState(shared_ptr<DiskCache> cache_p) : cache(std::move(cache_p)) {
	}

	~DiskCachePrefetchGlobalState() override {
		if (!cache || ranges.empty()) {
			return;
		}
		// Sort ranges by lru_rank (low to high)
		std::sort(ranges.begin(), ranges.end(),
		          [](const PrefetchRange &a, const PrefetchRange &b) { return a.lru_rank < b.lru_rank; });
		// Issue reads in rank order
		for (auto &range : ranges) {
			DiskCacheReadJob job;
			job.key = range.key;
			job.uri = range.uri;
			job.range_start = range.start;
			job.range_size = range.end - range.start;
			cache->QueueIORead(job);
		}
	}

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> DiskCachePrefetchBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	// Return a boolean column indicating success
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");
	return nullptr;
}

static unique_ptr<GlobalTableFunctionState> DiskCachePrefetchInitGlobal(ClientContext &context,
                                                                        TableFunctionInitInput &input) {
	auto cache = GetOrCreateDiskCache(*context.db);
	return make_uniq<DiskCachePrefetchGlobalState>(cache);
}

static OperatorResultType DiskCachePrefetchFunction(ExecutionContext &context, TableFunctionInput &data_p,
                                                    DataChunk &input, DataChunk &output) {
	auto &global_state = data_p.global_state->Cast<DiskCachePrefetchGlobalState>();

	if (!global_state.cache || !global_state.cache->disk_cache_initialized) {
		output.SetCardinality(0);
		return OperatorResultType::FINISHED;
	}
	auto &rank_vec = input.data[0];
	auto &uri_vec = input.data[1];
	auto &start_vec = input.data[2];
	auto &size_vec = input.data[3];
	auto count = input.size();

	// Flatten vectors to process them
	UnifiedVectorFormat rank_data, uri_data, start_data, size_data;
	rank_vec.ToUnifiedFormat(count, rank_data);
	uri_vec.ToUnifiedFormat(count, uri_data);
	start_vec.ToUnifiedFormat(count, start_data);
	size_vec.ToUnifiedFormat(count, size_data);

	auto rank_ptr = UnifiedVectorFormat::GetData<int64_t>(rank_data);
	auto uri_ptr = UnifiedVectorFormat::GetData<string_t>(uri_data);
	auto start_ptr = UnifiedVectorFormat::GetData<int64_t>(start_data);
	auto size_ptr = UnifiedVectorFormat::GetData<int64_t>(size_data);

	// Group ranges by uri and concatenate nearby ranges
	unordered_map<string, vector<PrefetchRange>> ranges_by_file;

	for (idx_t i = 0; i < count; i++) {
		auto rank_idx = rank_data.sel->get_index(i);
		auto uri_idx = uri_data.sel->get_index(i);
		auto start_idx = start_data.sel->get_index(i);
		auto size_idx = size_data.sel->get_index(i);

		if (!rank_data.validity.RowIsValid(rank_idx) || !uri_data.validity.RowIsValid(uri_idx) ||
		    !start_data.validity.RowIsValid(start_idx) || !size_data.validity.RowIsValid(size_idx)) {
			continue; // Skip null rows
		}
		idx_t lru_rank = rank_ptr[rank_idx];
		string uri = uri_ptr[uri_idx].GetString();
		idx_t range_start = start_ptr[start_idx];
		idx_t range_size = size_ptr[size_idx];

		if (range_size == 0) {
			continue;
		}
		auto &ranges = ranges_by_file[uri];
		PrefetchRange new_range;
		new_range.uri = uri;
		new_range.key = global_state.cache->GenCacheKey(uri);
		new_range.lru_rank = lru_rank;
		new_range.start = range_start;
		new_range.end = range_start + range_size;
		new_range.original_size = range_size;

		// Try to concatenate with the last range
		if (!ranges.empty()) {
			auto &last = ranges.back();
			idx_t concatenated_size = new_range.end - last.start;

			// Concatenate if concatenated_size seems cheaper to fetch than the two unconcatenated ranges
			if (EstimateS3(concatenated_size) < EstimateS3(last.original_size) + EstimateS3(new_range.original_size)) {
				last.end = new_range.end;
				last.original_size += new_range.original_size;
				last.lru_rank = std::min(last.lru_rank, new_range.lru_rank); // Use minimum rank
				continue;
			}
		}
		ranges.push_back(new_range);
	}

	// Add all ranges to global state for deferred execution
	std::lock_guard<std::mutex> lock(global_state.mutex);
	for (auto &entry : ranges_by_file) {
		auto &ranges = entry.second;
		for (auto &range : ranges) {
			global_state.ranges.push_back(range);
		}
	}

	// Return TRUE for all input rows
	output.SetCardinality(count);
	auto &result_vec = output.data[0];
	result_vec.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<bool>(result_vec)[0] = true;
	ConstantVector::SetNull(result_vec, false);

	return OperatorResultType::NEED_MORE_INPUT;
}

//===----------------------------------------------------------------------===//
// DiskCacheExtensionCallback - Automatic wrapping when target extensions load
//===----------------------------------------------------------------------===//
class DiskCacheExtensionCallback : public ExtensionCallback {
public:
	void OnExtensionLoaded(DatabaseInstance &db, const string &name) override {
		auto extension_name = StringUtil::Lower(name);
		if (extension_name == "httpfs" || extension_name == "azure") {
			DUCKDB_LOG_DEBUG(db, "[DiskCache] Target extension '%s' loaded, automatically wrapping filesystems",
			                 name.c_str());
			WrapExistingFilesystems(db);
		}
	}
};

void DiskCacheExtension::Load(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] DiskCache extension loaded!");
	// Get configuration for callbacks
	auto &config = DBConfig::GetConfig(instance);

	// Register table functions
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Registering table functions...");

	// Register disk_cache_config table function (supports 0, 1, 2, 3, or 4 arguments)
	TableFunction disk_cache_config_function("disk_cache_config", {}, DiskCacheConfigFunction);
	disk_cache_config_function.bind = DiskCacheConfigBind;
	disk_cache_config_function.init_global = DiskCacheConfigInitGlobal;
	disk_cache_config_function.varargs = LogicalType::ANY; // Allow variable arguments
	loader.RegisterFunction(disk_cache_config_function);
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Registered disk_cache_config function");

	// Register disk_cache_stats table function
	TableFunction disk_cache_stats_function("disk_cache_stats", {}, DiskCacheStatsFunction);
	disk_cache_stats_function.bind = DiskCacheStatsBind;
	disk_cache_stats_function.init_global = DiskCacheStatsInitGlobal;
	loader.RegisterFunction(disk_cache_stats_function);
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Registered disk_cache_stats function");

	// Register disk_cache_prefetch table function (TITO function)
	TableFunction disk_cache_prefetch_function(
	    "disk_cache_prefetch", {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT},
	    nullptr, DiskCachePrefetchBind, DiskCachePrefetchInitGlobal);
	disk_cache_prefetch_function.in_out_function = DiskCachePrefetchFunction;
	loader.RegisterFunction(disk_cache_prefetch_function);
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Registered disk_cache_prefetch function");

	/// create an initial cache
	idx_t max_size_mb, nr_io_threads;
	default_cache_sizes(instance, max_size_mb, nr_io_threads);
	auto shared_cache = GetOrCreateDiskCache(instance);
	shared_cache->path_sep = instance.GetFileSystem().PathSeparator("");
	shared_cache->ConfigureCache(".disk_cache", max_size_mb * 1024 * 1024, nr_io_threads);

	// Register extension callback for automatic wrapping
	config.extension_callbacks.push_back(make_uniq<DiskCacheExtensionCallback>());

	// Wrap any existing filesystems (in case some were already loaded)
	WrapExistingFilesystems(instance);
	DUCKDB_LOG_DEBUG(instance, "[DiskCache] Extension initialization complete!");
}

} // namespace duckdb

#ifdef DUCKDB_BUILD_LOADABLE_EXTENSION
extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(disk_cache, loader) {
	duckdb::DiskCacheExtension extension;
	extension.Load(loader);
}
}
#endif

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
