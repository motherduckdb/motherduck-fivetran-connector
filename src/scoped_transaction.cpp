#include "scoped_transaction.hpp"

ScopedTransaction::ScopedTransaction(duckdb::Connection& con_) : con(con_), owns_transaction(false) {
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

	// An active transaction in auto-commit mode belongs to DuckDB's per-statement handling, not to this
	// scope.
	if (con.HasActiveTransaction() && !con.IsAutoCommit()) {
		con.Rollback();
	}
}
