package kmysql.metadata

import kmysql.metadata.TableManager.Companion.MAX_NAME
import kmysql.record.Schema
import kmysql.record.TableScan
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

class IndexManager(
    private val isNew: Boolean,
    private val tableManager: TableManager,
    private val statisticsManager: StatisticsManager,
    private val transaction: Transaction,
) {

    init {
        if (isNew) {
            ConsoleLogger.info("IndexManager: indexcatalog 테이블을 생성합니다.")
            val schema = Schema()
            schema.addStringField("indexname", MAX_NAME)
            schema.addStringField("tablename", MAX_NAME)
            schema.addStringField("fieldname", MAX_NAME)
            tableManager.createTable("indexcatalog", schema, transaction)
            ConsoleLogger.info("IndexManager: indexcatalog 테이블 생성 완료")
        } else {
            ConsoleLogger.info("IndexManager: 기존 indexcatalog 테이블을 사용합니다.")
            try {
                tableManager.getLayout("indexcatalog", transaction)
                ConsoleLogger.info("IndexManager: indexcatalog 테이블 확인 완료")
            } catch (e: Exception) {
                ConsoleLogger.info("IndexManager: indexcatalog 테이블이 없어서 생성합니다.")
                val schema = Schema()
                schema.addStringField("indexname", MAX_NAME)
                schema.addStringField("tablename", MAX_NAME)
                schema.addStringField("fieldname", MAX_NAME)
                tableManager.createTable("indexcatalog", schema, transaction)
                ConsoleLogger.info("IndexManager: indexcatalog 테이블 생성 완료")
            }
        }
    }

    fun createIndex(indexName: String, tableName: String, fieldName: String, tx: Transaction) {
        val layout = tableManager.getLayout("indexcatalog", transaction)
        val tableScan = TableScan(tx, "indexcatalog", layout)
        tableScan.insert()
        tableScan.setString("indexname", indexName)
        tableScan.setString("tablename", tableName)
        tableScan.setString("fieldname", fieldName)
        tableScan.close()
    }

    fun getIndexInfo(tableName: String, tx: Transaction): Map<String, IndexInfo> {
        val result = mutableMapOf<String, IndexInfo>()
        val layout = tableManager.getLayout("indexcatalog", transaction)
        val tableScan = TableScan(tx, "indexcatalog", layout)
        while (tableScan.next()) {
            if (tableScan.getString("tablename") == tableName) {
                val indexName = tableScan.getString("indexname")
                val fieldName = tableScan.getString("fieldname")
                val tableLayout = tableManager.getLayout(tableName, tx)
                val tableStatisticsInformation = statisticsManager.getStatisticsInformation(tableName, tableLayout, tx)
                val indexInfo = IndexInfo(indexName, fieldName, tx, tableLayout.schema(), tableStatisticsInformation)
                result[fieldName] = indexInfo
            }
        }
        tableScan.close()
        return result
    }
}
