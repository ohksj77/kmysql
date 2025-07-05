package kmysql.server

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.FileManager
import kmysql.index.planner.IndexUpdatePlanner
import kmysql.metadata.MetadataManager
import kmysql.opt.HeuristicQueryPlanner
import kmysql.plan.BasicQueryPlanner
import kmysql.plan.BasicUpdatePlanner
import kmysql.plan.Planner
import kmysql.plan.QueryPlanner
import kmysql.plan.UpdatePlanner
import kmysql.transaction.Transaction
import kmysql.transaction.TransactionConfig
import kmysql.transaction.concurrency.IsolationLevel
import kmysql.util.ConsoleLogger
import java.io.File

const val BLOCK_SIZE = 400
const val BUFFER_SIZE = 8
const val LOG_FILE = "kmysql.log"

class KMySQL(
    directoryName: String,
    blockSize: Int = BLOCK_SIZE,
    bufferSize: Int = BUFFER_SIZE
) {
    val fileManager: FileManager = FileManager(File(directoryName), blockSize)
    val logManager: LogManager = LogManager(fileManager, LOG_FILE)
    val bufferManager: BufferManager = BufferManager(fileManager, logManager, bufferSize)
    val isNew: Boolean
    val effectiveIsNew: Boolean
    val planner: Planner
    private val metadataManager: MetadataManager

    init {
        val transaction = newTransaction()
        isNew = fileManager.isNew
        if (isNew) {
            ConsoleLogger.info("creating new database")
        } else {
            ConsoleLogger.info("recovering existing database")
            transaction.recover()
        }

        val shouldCreateSystemTables = try {
            val tableCatalogFile = fileManager.length("tablecatalog.tbl")
            ConsoleLogger.info("시스템 테이블들이 존재합니다.")
            false
        } catch (e: Exception) {
            ConsoleLogger.info("시스템 테이블들이 존재하지 않습니다. 생성합니다.")
            true
        }

        effectiveIsNew = isNew || shouldCreateSystemTables
        ConsoleLogger.info("effectiveIsNew: $effectiveIsNew")

        metadataManager = MetadataManager(effectiveIsNew, transaction)
        planner = createPlanner(metadataManager, true, true)
        transaction.commit()
    }

    fun getMetadataManager(transaction: Transaction): MetadataManager {
        return metadataManager
    }

    fun createPlanner(
        metadataManager: MetadataManager,
        useHeuristicQueryPlanner: Boolean = false,
        useIndexUpdatePlanner: Boolean = false
    ): Planner {
        val queryPlanner: QueryPlanner = if (useHeuristicQueryPlanner) {
            HeuristicQueryPlanner(metadataManager)
        } else {
            BasicQueryPlanner(metadataManager)
        }
        val updatePlanner: UpdatePlanner = if (useIndexUpdatePlanner) {
            IndexUpdatePlanner(metadataManager)
        } else {
            BasicUpdatePlanner(metadataManager)
        }
        return Planner(queryPlanner, updatePlanner)
    }

    fun newTransaction(isolationLevel: IsolationLevel = IsolationLevel.READ_COMMITTED): Transaction {
        val config = TransactionConfig(isolationLevel = isolationLevel)
        return Transaction(fileManager, bufferManager, logManager, config)
    }
}
