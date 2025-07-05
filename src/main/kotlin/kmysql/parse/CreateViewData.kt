package kmysql.parse

class CreateViewData(
    val viewName: String,
    private val queryData: QueryData,
) {
    fun viewDef(): String {
        return queryData.toString()
    }
}
