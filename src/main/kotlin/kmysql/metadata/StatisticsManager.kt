package kmysql.metadata

import kmysql.record.Layout
import kmysql.record.TableScan
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger
import java.util.concurrent.locks.ReentrantReadWriteLock

class StatisticsManager(
    private val tableManager: TableManager,
    private val transaction: Transaction,
) {
    private val tableStatistics = HashMap<String, StatisticsInformation>()
    private val lock = ReentrantReadWriteLock()
    private var numberCalls = 0

    fun getStatisticsInformation(tableName: String, layout: Layout, transaction: Transaction): StatisticsInformation {
        lock.readLock().lock()
        try {
            if (tableStatistics.containsKey(tableName)) {
                return tableStatistics[tableName]!!
            }
        } finally {
            lock.readLock().unlock()
        }

        lock.writeLock().lock()
        try {
            numberCalls += 1
            if (numberCalls > 100) refreshStatistics(transaction)
            return tableStatistics.computeIfAbsent(tableName) {
                calcTableStatistics(tableName, layout, transaction)
            }
        } finally {
            lock.writeLock().unlock()
        }
    }

    private fun refreshStatistics(transaction: Transaction) {
        lock.writeLock().lock()
        try {
            numberCalls = 0
            try {
                val tableCatalogLayout = tableManager.getLayout(TABLE_CATALOG, transaction)
                val tableCatalog = TableScan(transaction, TABLE_CATALOG, tableCatalogLayout)
                try {
                    while (tableCatalog.next()) {
                        try {
                            val tableName = tableCatalog.getString(TABLE_NAME)
                            if (tableName.isNotEmpty()) {
                                val layout = tableManager.getLayout(tableName, transaction)
                                val statisticsInformation = calcTableStatistics(tableName, layout, transaction)
                                tableStatistics[tableName] = statisticsInformation
                            }
                        } catch (e: Exception) {
                            ConsoleLogger.warn("Warning: Failed to process table statistics: ${e.message}")
                        }
                    }
                } finally {
                    tableCatalog.close()
                }
            } catch (e: Exception) {
                ConsoleLogger.warn("Warning: Could not refresh statistics: ${e.message}")
            }
        } finally {
            lock.writeLock().unlock()
        }
    }

    private fun calcTableStatistics(
        tableName: String,
        layout: Layout,
        transaction: Transaction
    ): StatisticsInformation {
        var numberRecords = 0
        var numberBlocks = 0
        val tableScan = TableScan(transaction, tableName, layout)
        try {
            while (tableScan.next()) {
                numberRecords += 1
                numberBlocks = tableScan.getRid().blockNumber + 1
            }
        } finally {
            tableScan.close()
        }
        return StatisticsInformation(numberBlocks, numberRecords)
    }

    companion object {
        const val TABLE_CATALOG = "tablecatalog.tbl"
        const val TABLE_NAME = "tablename"
    }
}
