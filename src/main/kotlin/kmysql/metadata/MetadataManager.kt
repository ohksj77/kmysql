package kmysql.metadata

import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.transaction.Transaction

class MetadataManager(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    companion object {
        lateinit var tableManager: TableManager
        lateinit var viewManager: ViewManager
        lateinit var statisticsManager: StatisticsManager
        lateinit var indexManager: IndexManager
    }

    init {
        tableManager = TableManager(isNew, transaction)
        viewManager = ViewManager(isNew, tableManager, transaction)
        statisticsManager = StatisticsManager(tableManager, transaction)
        indexManager = IndexManager(isNew, tableManager, statisticsManager, transaction)
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        tableManager.createTable(tableName, schema, tx)
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        return tableManager.getLayout(tableName, tx)
    }

    fun createView(viewName: String, viewDef: String, tx: Transaction) {
        viewManager.createView(viewName, viewDef, tx)
    }

    fun getViewDef(viewName: String, tx: Transaction): String? {
        return viewManager.getViewDef(viewName, tx)
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        indexManager.createIndex(indexName, tableName, fieldName, tx)
    }

    fun getIndexInformation(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        return indexManager.getIndexInfo(tableName, tx)
    }

    fun getStatisticsInformation(tableName: String, layout: Layout, tx: Transaction): StatisticsInformation {
        return statisticsManager.getStatisticsInformation(tableName, layout, tx)
    }
}
