package kmysql.transaction.recovery

import LogManager
import kmysql.file.BlockId
import kmysql.file.Page
import kmysql.transaction.Transaction

class SetStringRecord(page: Page) : LogRecord {
    val transactionId: Long
    val offset: Int
    val value: String
    val blockId: BlockId

    init {
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
        value = page.getString(valuePos)
    }

    override fun op(): Int = Operator.SET_STRING.id

    override fun transactionId(): Long = transactionId

    override fun toString(): String = "<SETSTRING $transactionId $blockId $offset $value>"

    override fun undo(transaction: Transaction) {
        transaction.pin(blockId)
        val buffer = transaction.getBuffer(blockId)
        if (buffer != null) {
            val page = buffer.contents()
            page.setString(offset, value)
            buffer.setModified(transaction.getTransactionId(), -1)
            buffer.forceFlush()
            transaction.getVersionManager().rollbackTo(blockId, transaction.getTransactionId())
        }
        transaction.unpin(blockId)
    }

    companion object {
        fun writeToLog(
            logManager: LogManager,
            transactionId: Long,
            blockId: BlockId,
            offset: Int,
            value: String
        ): Int {
            val transactionPos = java.lang.Long.BYTES
            val filePos = transactionPos + java.lang.Long.BYTES
            val blockPos = filePos + Page.maxLength(blockId.filename.length)
            val offsetPos = blockPos + Integer.BYTES
            val valuePos = offsetPos + Integer.BYTES
            val recordLength = valuePos + Page.maxLength(value.length)
            val record = ByteArray(recordLength)
            val page = Page(record)
            page.setInt(0, Operator.SET_STRING.id)
            page.setLong(transactionPos, transactionId)
            page.setString(filePos, blockId.filename)
            page.setInt(blockPos, blockId.number)
            page.setInt(offsetPos, offset)
            page.setString(valuePos, value)
            return logManager.append(record)
        }
    }
}
