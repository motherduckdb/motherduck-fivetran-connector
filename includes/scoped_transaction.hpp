#pragma once

#include "duckdb.hpp"

/// RAII wrapper around a DuckDB transaction. It only takes ownership if the connection does not have an
/// active transaction yet, so nesting it inside an outer transaction leaves that outer transaction alone.
/// Work is kept only when Commit() is called; anything left when the scope ends is rolled back.
struct ScopedTransaction {
	explicit ScopedTransaction(duckdb::Connection& con_);
	~ScopedTransaction();

	ScopedTransaction(const ScopedTransaction&) = delete;
	ScopedTransaction& operator=(const ScopedTransaction&) = delete;
	ScopedTransaction(ScopedTransaction&&) = delete;
	ScopedTransaction& operator=(ScopedTransaction&&) = delete;

	void Commit();

private:
	duckdb::Connection& con;
	bool owns_transaction;
};
