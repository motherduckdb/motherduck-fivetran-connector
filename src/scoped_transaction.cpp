#include "scoped_transaction.hpp"

#include <exception>
#include <string>

ScopedTransaction::ScopedTransaction(duckdb::Connection& con_, const mdlog::Logger& logger_)
    : con(con_), logger(logger_), owns_transaction(false) {
	if (!con.HasActiveTransaction()) {
		con.BeginTransaction();
		owns_transaction = true;
	}
}

void ScopedTransaction::Commit() {
	if (owns_transaction) {
		con.Commit();
		owns_transaction = false;
	}
}

ScopedTransaction::~ScopedTransaction() {
	// Only roll back a transaction this scope started. A transaction owned by an outer scope is expected
	// to stay active.
	if (!owns_transaction) {
		return;
	}

	// Rollback throws when the connection is no longer usable, e.g. after a fatal error or a failed remote
	// rollback. We catch this because destructors are implicitly noexcept.
	try {
		// An active transaction in auto-commit mode belongs to DuckDB's per-statement handling, not to this
		// scope.
		if (con.HasActiveTransaction() && !con.IsAutoCommit()) {
			con.Rollback();
		}
	} catch (const std::exception& ex) {
		logger.warning("Failed to roll back transaction: " + std::string(ex.what()));
	} catch (...) {
		logger.warning("Failed to roll back transaction with an unknown exception");
	}
}
