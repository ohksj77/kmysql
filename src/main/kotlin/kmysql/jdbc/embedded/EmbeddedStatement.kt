package kmysql.jdbc.embedded

import kmysql.jdbc.StatementAdapter
import kmysql.plan.Plan
import kmysql.plan.Planner
import kmysql.util.ConsoleLogger
import java.sql.ResultSet
import java.sql.SQLException

class EmbeddedStatement(
    private val connection: EmbeddedConnection,
    private val planner: Planner,
) : StatementAdapter() {
    override fun executeQuery(sql: String?): ResultSet {
        try {
            val transaction = connection.currentTransaction
            val cmd = sql ?: throw RuntimeException("null sql error")
            
            if (transaction == null) {
                throw RuntimeException("Transaction is null")
            }
            
            val plan: Plan = planner.createQueryPlan(cmd, transaction)
            return EmbeddedResultSet(plan, connection)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun executeUpdate(sql: String?): Int {
        try {
            val transaction = connection.currentTransaction
            val cmd = sql ?: throw RuntimeException("null sql error")
            
            if (transaction == null) {
                throw RuntimeException("Transaction is null")
            }
            
            if (planner == null) {
                throw RuntimeException("Planner is null")
            }
            
            val result = planner.executeUpdate(cmd, transaction)
            return result
        } catch (e: RuntimeException) {
            ConsoleLogger.error(e.message ?: "Unknown error")
            connection.rollback()
            throw SQLException(e)
        }
    }

    fun executeUpdateWithoutCommit(sql: String?): Int {
        try {
            val transaction = connection.currentTransaction
            val cmd = sql ?: throw RuntimeException("null sql error")
            
            if (transaction == null) {
                throw RuntimeException("Transaction is null")
            }
            
            val result = planner.executeUpdate(cmd, transaction)
            return result
        } catch (e: RuntimeException) {
            ConsoleLogger.info(e.message ?: "Unknown error")
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun close() {}

    override fun getConnection(): EmbeddedConnection {
        return connection
    }
}
