package kmysql.jdbc.embedded

import kmysql.jdbc.ResultSetMetaDataAdapter
import kmysql.record.Schema

class EmbeddedMetaData(
    private val schema: Schema,
) : ResultSetMetaDataAdapter() {
    override fun getColumnCount(): Int {
        return schema.fields.size
    }

    override fun getColumnName(column: Int): String {
        return schema.fields[column - 1]
    }

    override fun getColumnType(column: Int): Int {
        val fieldName = getColumnName(column)
        return schema.type(fieldName) ?: throw RuntimeException("null error")
    }

    override fun getColumnDisplaySize(column: Int): Int {
        val fieldName = getColumnName(column)
        val fieldType = schema.type(fieldName) ?: throw RuntimeException("null error")
        val fieldLength = if (fieldType == java.sql.Types.INTEGER) {
            6
        } else {
            schema.length(fieldName)
        } ?: throw RuntimeException("null error")
        return fieldName.length.coerceAtLeast(fieldLength) + 1
    }

    override fun getColumnTypeName(column: Int): String {
        val fieldType = getColumnType(column)
        return when (fieldType) {
            java.sql.Types.INTEGER -> "INTEGER"
            java.sql.Types.VARCHAR -> "VARCHAR"
            else -> "UNKNOWN"
        }
    }
}
