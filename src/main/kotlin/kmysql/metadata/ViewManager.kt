package kmysql.metadata

import kmysql.record.TableScan
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

class ViewManager(
    private val isNew: Boolean,
    private val tableManager: TableManager,
    private val transaction: Transaction,
) {
    init {
        ConsoleLogger.info("ViewManager: viewcatalog 테이블 생성을 건너뜁니다.")
    }

    fun createView(viewName: String, viewDef: String, transaction: Transaction) {
        val layout = tableManager.getLayout(VIEW_CATALOG_NAME, transaction)
        TableScan(transaction, VIEW_CATALOG_NAME, layout).use { tableScan ->
            tableScan.insert()
            tableScan.setString(VIEW_NAME_FIELD, viewName)
            tableScan.setString(VIEW_DEF_FIELD, viewDef)
        }
    }

    fun getViewDef(viewName: String, transaction: Transaction): String? {
        try {
            val layout = tableManager.getLayout(VIEW_CATALOG_NAME, transaction)
            TableScan(transaction, VIEW_CATALOG_NAME, layout).use { tableScan ->
                while (tableScan.next()) {
                    if (tableScan.getString(VIEW_NAME_FIELD) == viewName) {
                        return tableScan.getString(VIEW_DEF_FIELD)
                    }
                }
            }
        } catch (e: Exception) {
            ConsoleLogger.info("ViewManager: viewcatalog 테이블이 없어서 null 반환")
        }
        return null
    }

    companion object {
        private const val VIEW_CATALOG_NAME = "viewcatalog"
        private const val VIEW_NAME_FIELD = "viewname"
        private const val VIEW_DEF_FIELD = "viewdef"
        private const val MAX_VIEW_DEF = 100
    }
}
