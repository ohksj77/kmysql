package kmysql.transaction

import LogManager
import kmysql.buffer.Buffer
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.record.Rid
import kmysql.transaction.concurrency.ConcurrencyManager
import kmysql.transaction.concurrency.RecordLockManager
import kmysql.transaction.concurrency.VersionManager
import kmysql.transaction.recovery.RecoveryManager
import kmysql.transaction.recovery.ReleaseSavepointRecord
import kmysql.transaction.recovery.SavepointRecord
import kmysql.util.ConsoleLogger
import java.util.concurrent.ConcurrentHashMap

class Transaction(
    val fm: FileManager,
    val bm: BufferManager,
    val lm: LogManager,
    private val config: TransactionConfig = TransactionConfig.kmysqlDefault()
) {
    private val isolationLevel = config.isolationLevel
    private val autoCommit = config.autoCommit
    private val endOfFile = -1
    private val rm: RecoveryManager
    private val cm: ConcurrencyManager
    private val vm: VersionManager = VersionManager.getInstance()
    private val rlm: RecordLockManager = RecordLockManager.getInstance()
    private val transactionId: Long = generateTransactionId()
    private val buffers: BufferList
    private val readTimestamp: Long = vm.getNextTimestamp()
    private val txStartTimestamp: Long = readTimestamp

    private val savePoints = ConcurrentHashMap<String, Savepoint>()
    private var savepointCounter = 0

    private var isAutoCommitMode = autoCommit
    private var hasUncommittedChanges = false
    private var isCommitted = false
    private var isRolledBack = false
    private var isRollingBack = false

    init {
        rm = RecoveryManager(this, transactionId, lm, bm)
        cm = ConcurrencyManager(transactionId, isolationLevel)
        buffers = BufferList(bm)

        cm.setLockTimeout(config.lockTimeout)
    }

    private fun generateTransactionId(): Long {
        return TransactionIdGenerator.generateTransactionId()
    }

    fun commit() {
        if (isCommitted || isRolledBack) {
            return
        }

        try {
            rm.commit()
            cm.release()
            buffers.unpinAll()

            bm.flushAll(transactionId)

            isCommitted = true
            hasUncommittedChanges = false
            savePoints.clear()
            vm.commitTransaction(transactionId)
            ConsoleLogger.info("transaction $transactionId committed")
            resetTransactionState()
        } catch (e: Exception) {
            rollback()
            throw e
        }
    }

    fun rollback() {
        if (isCommitted || isRolledBack) return
        try {
            isRollingBack = true
            rm.rollback()
            cm.release()
            buffers.unpinAll()
            isRolledBack = true
            isRollingBack = false
            hasUncommittedChanges = false
            savePoints.clear()
            vm.rollbackTransaction(transactionId)
            ConsoleLogger.info("transaction $transactionId rolled back")
            resetTransactionState()
        } catch (e: Exception) {
            isRollingBack = false
            ConsoleLogger.error("Error during rollback: ${e.message}")
            throw e
        }
    }

    private fun resetTransactionState() {
        isCommitted = false
        isRolledBack = false
        isRollingBack = false
        hasUncommittedChanges = false
        savePoints.clear()
    }

    fun createSavepoint(name: String? = null): String {
        if (savePoints.size >= config.maxSavePoints) {
            throw RuntimeException("Maximum number of savepoints (${config.maxSavePoints}) exceeded")
        }

        val savepointName = name ?: "SP_${transactionId}_${++savepointCounter}"
        val savepoint = Savepoint(
            name = savepointName,
            transactionId = transactionId,
            timestamp = vm.getNextTimestamp()
        )
        savePoints[savepointName] = savepoint

        val lsn = SavepointRecord.writeToLog(lm, transactionId, savepointName)
        lm.flush(lsn)

        ConsoleLogger.info("Savepoint created: $savepointName")
        return savepointName
    }

    fun rollbackToSavepoint(savepointName: String) {
        val savepoint = savePoints[savepointName]
            ?: throw RuntimeException("Savepoint '$savepointName' does not exist")

        vm.rollbackToSavepoint(transactionId, savepoint.timestamp)

        rm.rollbackToSavepoint(savepoint)

        buffers.getAllBlockIds().forEach { blockId ->
            val buffer = buffers.getBuffer(blockId)
            if (buffer != null && buffer.modifyingTransaction() == transactionId) {
                val page = buffer.contents()
                vm.createVersion(blockId, page.toByteArray(), transactionId)
                ConsoleLogger.info("rollbackToSavepoint: created new version for blockId=$blockId after buffer restoration")
            }
        }

        val keysToRemove = savePoints.keys.filter { key ->
            val sp = savePoints[key]
            sp != null && sp.timestamp > savepoint.timestamp
        }
        keysToRemove.forEach { savePoints.remove(it) }

        ConsoleLogger.info("Rolled back to savepoint: $savepointName")
    }

    fun releaseSavepoint(savepointName: String) {
        if (savePoints.remove(savepointName) != null) {
            val lsn = ReleaseSavepointRecord.writeToLog(lm, transactionId, savepointName)
            lm.flush(lsn)

            ConsoleLogger.info("Savepoint released: $savepointName")
        } else {
            throw RuntimeException("Savepoint '$savepointName' does not exist")
        }
    }

    fun recover() {
        bm.flushAll(transactionId)
        rm.recover()
    }

    fun pin(blockId: BlockId) {
        buffers.pin(blockId)
    }

    fun unpin(blockId: BlockId) {
        buffers.unpin(blockId)
    }

    fun getInt(blockId: BlockId, offset: Int): Int? {
        if (offset < 0) return null
        if (isCommitted || isRolledBack) throw RuntimeException("Transaction is already committed or rolled back")
        if (isSystemTable(blockId)) {
            val buffer = buffers.getBuffer(blockId) ?: run {
                buffers.pin(blockId)
                buffers.getBuffer(blockId) ?: return null
            }
            return buffer.contents().getInt(offset)
        }
        val version = vm.getVisibleVersion(
            blockId,
            transactionId,
            readTimestamp,
            isolationLevel,
            txStartTimestamp
        )
        if (version != null) {
            val buffer = java.nio.ByteBuffer.wrap(version.data)
            buffer.position(offset)
            return buffer.int
        }
        return null
    }

    fun getString(blockId: BlockId, offset: Int): String? {
        if (offset < 0) return null
        if (isCommitted || isRolledBack) throw RuntimeException("Transaction is already committed or rolled back")
        if (isSystemTable(blockId)) {
            val buffer = buffers.getBuffer(blockId) ?: run {
                buffers.pin(blockId)
                buffers.getBuffer(blockId) ?: return null
            }
            return buffer.contents().getString(offset)
        }
        val version = vm.getVisibleVersion(
            blockId,
            transactionId,
            readTimestamp,
            isolationLevel,
            txStartTimestamp
        )
        if (version != null) {
            val buffer = java.nio.ByteBuffer.wrap(version.data)
            buffer.position(offset)
            val length = buffer.int
            val bytes = ByteArray(length)
            buffer.get(bytes)
            return String(bytes)
        }
        return null
    }

    private fun isSystemTable(blockId: BlockId): Boolean {
        val systemTables = setOf("tablecatalog", "fieldcatalog", "viewcatalog", "indexcatalog")
        return systemTables.any { blockId.filename == it } || blockId.filename.endsWith(".tbl")
    }

    fun setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        if (offset < 0) {
            return
        }

        if (isCommitted || isRolledBack) {
            throw RuntimeException("Transaction is already committed or rolled back")
        }

        val buffer = buffers.getBuffer(blockId) ?: run {
            buffers.pin(blockId)
            buffers.getBuffer(blockId) ?: return
        }
        val lsn = if (okToLog) {
            ConsoleLogger.info("setInt: creating log record for blockId=$blockId, offset=$offset")
            rm.setInt(buffer, offset)
        } else -1
        val page = buffer.contents()
        page.setInt(offset, value)
        buffer.setModified(transactionId, lsn)

        val version = vm.createVersion(blockId, page.toByteArray(), transactionId)
        ConsoleLogger.info("setInt: blockId=$blockId, offset=$offset, value=$value, transactionId=$transactionId, version=$version")
        hasUncommittedChanges = true

        if (isAutoCommitMode && okToLog) {
            buffer.flush()
            commit()
        }
    }

    fun setString(blockId: BlockId, offset: Int, value: String) {
        if (isCommitted || isRolledBack) {
            throw RuntimeException("Transaction is already committed or rolled back")
        }

        try {
            val buffer = buffers.getBuffer(blockId) ?: run {
                buffers.pin(blockId)
                buffers.getBuffer(blockId) ?: return
            }

            val page = buffer.contents()
            page.setString(offset, value)

            buffer.setModified(transactionId, -1)
            hasUncommittedChanges = true
        } catch (e: Exception) {
            ConsoleLogger.error("Error in setString: ${e.message}")
            throw e
        }
    }

    fun sLockRecord(rid: Rid) {
        rlm.sLockRecord(rid)
    }

    fun xLockRecord(rid: Rid) {
        rlm.xLockRecord(rid)
    }

    fun sUnlockRecord(rid: Rid) {
        rlm.sUnlockRecord(rid)
    }

    fun xUnlockRecord(rid: Rid) {
        rlm.xUnlockRecord(rid)
    }

    fun size(filename: String): Int {
        val dummyBlock = BlockId(filename, endOfFile)
        return fm.length(filename)
    }

    fun append(filename: String): BlockId {
        val dummyBlock = BlockId(filename, endOfFile)
        return fm.append(filename)
    }

    fun blockSize(): Int = fm.blockSize

    fun availableBuffers(): Int = bm.available()

    fun getTransactionId(): Long = transactionId

    fun isRollingBack(): Boolean = isRollingBack

    fun isActive(): Boolean = !isCommitted && !isRolledBack

    fun getTransactionInfo(): String {
        return """
            |Transaction Info:
            |  ID: $transactionId
            |  Number: $transactionId
            |  Isolation Level: ${isolationLevel.description}
            |  Auto Commit: $isAutoCommitMode
            |  Status: ${if (isActive()) "ACTIVE" else if (isCommitted) "COMMITTED" else "ROLLED BACK"}
            |  Has Uncommitted Changes: $hasUncommittedChanges
            |  Savepoint Count: ${savePoints.size}
            |  Lock Count: ${cm.getLockCount()}
            |  Lock Timeout: ${cm.getLockTimeout()}s
        """.trimMargin()
    }

    fun getBuffer(blockId: BlockId): Buffer? {
        return buffers.getBuffer(blockId)
    }

    fun flushBuffer(blockId: BlockId) {
        getBuffer(blockId)?.forceFlush()
    }

    fun getVersionManager(): VersionManager = vm
}

data class Savepoint(
    val name: String,
    val transactionId: Long,
    val timestamp: Long
)
