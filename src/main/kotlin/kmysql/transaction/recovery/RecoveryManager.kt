package kmysql.transaction.recovery

import LogManager
import kmysql.buffer.Buffer
import kmysql.buffer.BufferManager
import kmysql.transaction.Savepoint
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

class RecoveryManager(
    private val transaction: Transaction,
    private val transactionId: Long,
    private val logManager: LogManager,
    private val bufferManager: BufferManager
) {
    init {
        StartRecord.writeToLog(logManager, transactionId)
    }

    fun commit() {
        bufferManager.flushAll(transactionId)
        val lsn = CommitRecord.writeToLog(logManager, transactionId)
        logManager.flush(lsn)
    }

    fun rollback() {
        ConsoleLogger.info("RecoveryManager.rollback: transactionId=$transactionId")
        doRollback()
        val lsn = RollbackRecord.writeToLog(logManager, transactionId)
        logManager.flush(lsn)
        bufferManager.flushAll(transactionId)
        ConsoleLogger.info("RecoveryManager.rollback: completed")
    }

    fun rollbackToSavepoint(savepoint: Savepoint) {
        doRollbackToSavepoint(savepoint)
        val lsn = SavepointRollbackRecord.writeToLog(logManager, transactionId, savepoint.name)
        logManager.flush(lsn)
        bufferManager.flushAll(transactionId)
    }

    fun recover() {
        doRecover()
        bufferManager.flushAll(transactionId)
        val lsn = CheckpointRecord.writeToLog(logManager)
        logManager.flush(lsn)
    }

    fun setInt(buffer: Buffer, offset: Int): Int {
        val oldValue = buffer.contents().getInt(offset)
        val blockId = buffer.blockId() ?: throw RuntimeException("null error")
        ConsoleLogger.info("RecoveryManager.setInt: blockId=$blockId, offset=$offset, oldValue=$oldValue")
        return SetIntRecord.writeToLog(logManager, transactionId, blockId, offset, oldValue)
    }

    fun setString(buffer: Buffer, offset: Int): Int {
        val oldValue = buffer.contents().getString(offset)
        val blockId = buffer.blockId() ?: throw RuntimeException("null error")
        return SetStringRecord.writeToLog(logManager, transactionId, blockId, offset, oldValue)
    }

    private fun doRollback() {
        val iterator = logManager.reverseIterator()
        val recordsToUndo = mutableListOf<LogRecord>()

        ConsoleLogger.info("doRollback: starting rollback for transactionId=$transactionId")
        var totalRecords = 0
        var matchingRecords = 0

        while (iterator.hasNext()) {
            val bytes = iterator.next()
            totalRecords++
            val record = LogRecord.createLogRecord(bytes)
            if (record != null) {
                ConsoleLogger.info("doRollback: found record $record with transactionId=${record.transactionId()}")
                if (record.transactionId() == transactionId) {
                    matchingRecords++
                    if (record.op() == Operator.START.id) {
                        ConsoleLogger.info("doRollback: found START record, stopping")
                        break
                    } else {
                        ConsoleLogger.info("doRollback: add undo record $record")
                        recordsToUndo.add(record)
                    }
                }
            } else {
                ConsoleLogger.info("doRollback: could not create log record from bytes")
            }
        }

        ConsoleLogger.info("doRollback: total records=$totalRecords, matching records=$matchingRecords, records to undo=${recordsToUndo.size}")
        recordsToUndo.forEach { record ->
            ConsoleLogger.info("doRollback: undoing record $record")
            record.undo(transaction)
        }
    }

    private fun doRollbackToSavepoint(savepoint: Savepoint) {
        val iterator = logManager.reverseIterator()
        val recordsToUndo = mutableListOf<LogRecord>()

        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes)
            if (record != null && record.transactionId() == transactionId) {
                if (record.op() == Operator.START.id) break
                if (record is SavepointRecord && record.savepointName == savepoint.name) {
                    break
                }
                recordsToUndo.add(record)
            }
        }

        recordsToUndo.forEach { record ->
            record.undo(transaction)
        }
    }

    private fun doRecover() {
        val finishedTransaction = mutableListOf<Long>()
        val iterator = logManager.iterator()
        while (iterator.hasNext()) {
            val bytes = iterator.next()
            val record = LogRecord.createLogRecord(bytes) ?: return
            if (record.op() == Operator.CHECKPOINT.id) return

            if (record.op() == Operator.COMMIT.id || record.op() == Operator.ROLLBACK.id) {
                finishedTransaction.add(record.transactionId())
            } else if (!finishedTransaction.contains(record.transactionId())) {
                record.undo(transaction)
            }
        }
    }
}
