package kmysql.jdbc.embedded

import kmysql.jdbc.ConnectionAdapter
import kmysql.plan.Planner
import kmysql.server.KMySQL
import kmysql.transaction.concurrency.IsolationLevel
import java.sql.SQLException
import java.sql.Statement

class EmbeddedConnection(
    private val db: KMySQL,
) : ConnectionAdapter() {
    private var isolationLevel = IsolationLevel.REPEATABLE_READ
    var currentTransaction = db.newTransaction(isolationLevel)

    private fun getPlanner(): Planner {
        val metadataManager = db.getMetadataManager(currentTransaction)
        return db.createPlanner(metadataManager)
    }

    override fun createStatement(): Statement {
        return EmbeddedStatement(this, getPlanner())
    }

    override fun createStatement(resultSetType: Int, resultSetConcurrency: Int, resultSetHoldability: Int): Statement {
        throw SQLException("operation not implemented")
    }

    override fun close() {
        commit()
    }

    override fun commit() {
        currentTransaction.commit()
        currentTransaction = db.newTransaction(isolationLevel)
    }

    override fun rollback() {
        currentTransaction.rollback()
        currentTransaction = db.newTransaction(isolationLevel)
    }

    override fun setTransactionIsolation(level: Int) {
        isolationLevel = when (level) {
            java.sql.Connection.TRANSACTION_READ_UNCOMMITTED -> IsolationLevel.READ_UNCOMMITTED
            java.sql.Connection.TRANSACTION_READ_COMMITTED -> IsolationLevel.READ_COMMITTED
            java.sql.Connection.TRANSACTION_REPEATABLE_READ -> IsolationLevel.REPEATABLE_READ
            java.sql.Connection.TRANSACTION_SERIALIZABLE -> IsolationLevel.SERIALIZABLE
            else -> IsolationLevel.REPEATABLE_READ
        }
        currentTransaction = db.newTransaction(isolationLevel)
    }

    override fun getTransactionIsolation(): Int {
        return when (isolationLevel) {
            IsolationLevel.READ_UNCOMMITTED -> java.sql.Connection.TRANSACTION_READ_UNCOMMITTED
            IsolationLevel.READ_COMMITTED -> java.sql.Connection.TRANSACTION_READ_COMMITTED
            IsolationLevel.REPEATABLE_READ -> java.sql.Connection.TRANSACTION_REPEATABLE_READ
            IsolationLevel.SERIALIZABLE -> java.sql.Connection.TRANSACTION_SERIALIZABLE
        }
    }
}
