package kmysql.metadata

import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.transaction.Transaction

class MetadataManager(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    private val tableManager: TableManager = TableManager(isNew, transaction)
    private var viewManager: ViewManager? = null
    private var statisticsManager: StatisticsManager? = null
    private var indexManager: IndexManager? = null

    private fun getViewManager(): ViewManager {
        if (viewManager == null) {
            viewManager = ViewManager(isNew, tableManager, transaction)
        }
        return viewManager!!
    }

    private fun getStatisticsManager(): StatisticsManager {
        if (statisticsManager == null) {
            statisticsManager = StatisticsManager(tableManager, transaction)
        }
        return statisticsManager!!
    }

    private fun getIndexManager(): IndexManager {
        if (indexManager == null) {
            indexManager = IndexManager(isNew, tableManager, getStatisticsManager(), transaction)
        }
        return indexManager!!
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        tableManager.createTable(tableName, schema, tx)
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        return tableManager.getLayout(tableName, tx)
    }

    fun createView(viewName: String, viewDef: String, tx: Transaction) {
        getViewManager().createView(viewName, viewDef, tx)
    }

    fun getViewDef(viewName: String, tx: Transaction): String? {
        return getViewManager().getViewDef(viewName, tx)
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        getIndexManager().createIndex(indexName, tableName, fieldName, tx)
    }

    fun getIndexInformation(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        return getIndexManager().getIndexInfo(tableName, tx)
    }

    fun getStatisticsInformation(tableName: String, layout: Layout, tx: Transaction): StatisticsInformation {
        return getStatisticsManager().getStatisticsInformation(tableName, layout, tx)
    }
}
