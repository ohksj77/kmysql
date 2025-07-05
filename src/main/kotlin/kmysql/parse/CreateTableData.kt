package kmysql.parse

import kmysql.record.Schema

data class CreateTableData(
    val tableName: String,
    val schema: Schema,
)
