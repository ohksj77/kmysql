package kmysql.parse

data class CreateIndexData(
    val indexName: String,
    val tableName: String,
    val fieldName: String,
)
