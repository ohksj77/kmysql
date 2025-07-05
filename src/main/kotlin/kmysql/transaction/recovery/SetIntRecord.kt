package kmysql.transaction.recovery

import LogManager
import kmysql.file.BlockId
import kmysql.file.Page
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

class SetIntRecord(page: Page) : LogRecord {
    val transactionId: Long
    val offset: Int
    val value: Int
    val blockId: BlockId

    init {
        try {
            val transactionPos = java.lang.Long.BYTES
            transactionId = page.getLong(transactionPos)
            val filePos = transactionPos + java.lang.Long.BYTES
            val filename = page.getString(filePos)
            val blockPos = filePos + Page.maxLength(filename.length)
            val blockNumber = page.getInt(blockPos)
            blockId = BlockId(filename, blockNumber)
            val offsetPos = blockPos + Integer.BYTES
            offset = page.getInt(offsetPos)
            val valuePos = offsetPos + Integer.BYTES
            value = page.getInt(valuePos)

            ConsoleLogger.info("SetIntRecord.init: successfully created record for transactionId=$transactionId, blockId=$blockId, offset=$offset, value=$value")
        } catch (e: Exception) {
            ConsoleLogger.error("SetIntRecord.init: failed to create record: ${e.message}")
            throw e
        }
    }

    override fun op(): Int = Operator.SET_INT.id

    override fun transactionId(): Long = transactionId

    override fun toString(): String = "<SETINT $transactionId $blockId $offset $value>"

    override fun undo(transaction: Transaction) {
        ConsoleLogger.info("SetIntRecord.undo: blockId=$blockId, offset=$offset, value=$value")
        transaction.pin(blockId)
        val buffer = transaction.getBuffer(blockId)
        if (buffer != null) {
            val page = buffer.contents()
            page.setInt(offset, value)
            buffer.setModified(transaction.getTransactionId(), -1)

            if (transaction.isRollingBack()) {
                buffer.forceFlush()
                transaction.getVersionManager().rollbackTo(blockId, transaction.getTransactionId())
                ConsoleLogger.info("SetIntRecord.undo: buffer restored and flushed (full transaction rollback)")
            } else {
                ConsoleLogger.info("SetIntRecord.undo: buffer restored (savepoint rollback)")
            }
        } else {
            ConsoleLogger.info("SetIntRecord.undo: buffer is null")
        }
        transaction.unpin(blockId)
    }

    companion object {
        fun writeToLog(
            logManager: LogManager,
            transactionId: Long,
            blockId: BlockId,
            offset: Int,
            value: Int
        ): Int {
            val transactionPos = java.lang.Long.BYTES
            val filePos = transactionPos + java.lang.Long.BYTES
            val blockPos = filePos + Page.maxLength(blockId.filename.length)
            val offsetPos = blockPos + Integer.BYTES
            val valuePos = offsetPos + Integer.BYTES
            val record = ByteArray(valuePos + Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.SET_INT.id)
            page.setLong(transactionPos, transactionId)
            page.setString(filePos, blockId.filename)
            page.setInt(blockPos, blockId.number)
            page.setInt(offsetPos, offset)
            page.setInt(valuePos, value)

            val lsn = logManager.append(record)
            ConsoleLogger.info("SetIntRecord.writeToLog: created record with lsn=$lsn, transactionId=$transactionId, blockId=$blockId, offset=$offset, value=$value")
            return lsn
        }
    }
}
