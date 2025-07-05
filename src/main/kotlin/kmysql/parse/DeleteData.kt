package kmysql.parse

import kmysql.query.Predicate

data class DeleteData(
    val tableName: String,
    val predicate: Predicate,
)
