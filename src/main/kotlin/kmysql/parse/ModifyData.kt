package kmysql.parse

import kmysql.query.Expression
import kmysql.query.Predicate

class ModifyData(
    val tableName: String,
    val fieldName: String,
    val newValue: Expression,
    val predicate: Predicate,
)
