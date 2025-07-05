package kmysql.jdbc.embedded

import kmysql.jdbc.ResultSetAdapter
import kmysql.plan.Plan
import kmysql.query.Scan
import java.sql.ResultSetMetaData
import java.sql.SQLException
import java.util.*

class EmbeddedResultSet(
    private val plan: Plan,
    private val connection: EmbeddedConnection,
) : ResultSetAdapter() {
    private val scan: Scan = plan.open()
    private val schema = plan.schema()

    override fun next(): Boolean {
        try {
            return scan.next()
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getInt(columnLabel: String?): Int {
        try {
            val fieldName = columnLabel?.lowercase(Locale.getDefault()) ?: throw RuntimeException("null error")
            return scan.getInt(fieldName)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getInt(columnIndex: Int): Int {
        try {
            val fieldName = schema.fields[columnIndex - 1]
            return scan.getInt(fieldName)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getString(columnLabel: String?): String {
        try {
            val fieldName = columnLabel?.lowercase(Locale.getDefault()) ?: throw RuntimeException("null error")
            return scan.getString(fieldName)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getString(columnIndex: Int): String {
        try {
            val fieldName = schema.fields[columnIndex - 1]
            return scan.getString(fieldName)
        } catch (e: RuntimeException) {
            connection.rollback()
            throw SQLException(e)
        }
    }

    override fun getMetaData(): ResultSetMetaData {
        return EmbeddedMetaData(schema)
    }

    override fun close() {
        scan.close()
        connection.commit()
    }
}
