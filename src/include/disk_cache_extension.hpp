#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/config.hpp"
#include "disk_cache.hpp"
#include "disk_cache_fs_wrapper.hpp"

namespace duckdb {

class DiskCacheExtension : public Extension {
public:
	void Load(ExtensionLoader &loader) override;
	std::string Name() override {
		return "disk_cache";
	}
	std::string Version() const override {
		return "0.1";
	}
};

} // namespace duckdb
