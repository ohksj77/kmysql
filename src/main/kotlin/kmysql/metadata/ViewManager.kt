package kmysql.metadata

import kmysql.metadata.TableManager.Companion.MAX_NAME
import kmysql.record.Schema
import kmysql.record.TableScan
import kmysql.transaction.Transaction

class ViewManager(
    private val isNew: Boolean,
    private val tableManager: TableManager,
    private val transaction: Transaction,
) {
    init {
        if (isNew) {
            val schema = Schema()
            schema.addStringField(VIEW_NAME_FIELD, MAX_NAME)
            schema.addStringField(VIEW_DEF_FIELD, MAX_VIEW_DEF)
            tableManager.createTable(VIEW_CATALOG_NAME, schema, transaction)
        }
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
        val layout = tableManager.getLayout(VIEW_CATALOG_NAME, transaction)
        TableScan(transaction, VIEW_CATALOG_NAME, layout).use { tableScan ->
            while (tableScan.next()) {
                if (tableScan.getString(VIEW_NAME_FIELD) == viewName) {
                    return tableScan.getString(VIEW_DEF_FIELD)
                }
            }
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
