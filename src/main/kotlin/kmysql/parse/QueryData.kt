package kmysql.parse

import kmysql.query.Predicate

class QueryData(
    val fields: List<String>,
    val tables: Collection<String>,
    val predicate: Predicate,
) {
    override fun toString(): String {
        var result = "select "
        for (filedName in fields) {
            result += "$filedName, "
        }
        result = result.substring(0, result.length - 2)
        result += " from "
        for (tableName in tables) {
            result += "$tableName, "
        }
        result = result.substring(0, result.length - 2)
        val predicateString = predicate.toString()
        if (predicateString != "") {
            result += " where $predicateString"
        }
        return result
    }
}
