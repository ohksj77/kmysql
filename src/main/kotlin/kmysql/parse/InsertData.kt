package kmysql.parse

import kmysql.query.Constant

data class InsertData(
    val tableName: String,
    val fields: List<String>,
    val values: List<Constant>,
)
