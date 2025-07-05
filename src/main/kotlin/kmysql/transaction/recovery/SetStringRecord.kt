package kmysql.transaction.recovery

import LogManager
import kmysql.file.BlockId
import kmysql.file.Page
import kmysql.transaction.Transaction

class SetStringRecord(page: Page) : LogRecord {
    val transactionNumber: Int
    val offset: Int
    val value: String
    val blockId: BlockId

    init {
        val transactionPos = Integer.BYTES
        transactionNumber = page.getInt(transactionPos)
        val filePos = transactionPos + Integer.BYTES
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

    override fun transactionNumber(): Int = transactionNumber

    override fun toString(): String = "<SETSTRING $transactionNumber $blockId $offset $value>"

    override fun undo(transaction: Transaction) {
        transaction.pin(blockId)
        transaction.setString(blockId, offset, value, false)
        transaction.unpin(blockId)
    }

    companion object {
        fun writeToLog(
            logManager: LogManager,
            transactionNumber: Int,
            blockId: BlockId,
            offset: Int,
            value: String
        ): Int {
            val transactionPos = Integer.BYTES
            val filePos = transactionPos + Integer.BYTES
            val blockPos = filePos + Page.maxLength(blockId.filename.length)
            val offsetPos = blockPos + Integer.BYTES
            val valuePos = offsetPos + Integer.BYTES
            val recordLength = valuePos + Page.maxLength(value.length)
            val record = ByteArray(recordLength)
            val page = Page(record)
            page.setInt(0, Operator.SET_STRING.id)
            page.setInt(transactionPos, transactionNumber)
            page.setString(filePos, blockId.filename)
            page.setInt(blockPos, blockId.number)
            page.setInt(offsetPos, offset)
            page.setString(valuePos, value)
            return logManager.append(record)
        }
    }
}
