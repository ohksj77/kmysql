package kmysql.transaction.recovery

import LogManager
import kmysql.file.BlockId
import kmysql.file.Page
import kmysql.transaction.Transaction

class SetIntRecord(page: Page) : LogRecord {
    val transactionNumber: Int
    val offset: Int
    val value: Int
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
        value = page.getInt(valuePos)
    }

    override fun op(): Int = Operator.SET_INT.id

    override fun transactionNumber(): Int = transactionNumber

    override fun toString(): String = "<SETINT $transactionNumber $blockId $offset $value>"

    override fun undo(transaction: Transaction) {
        transaction.pin(blockId)
        transaction.setInt(blockId, offset, value, false)
        transaction.unpin(blockId)
    }

    companion object {
        fun writeToLog(
            logManager: LogManager,
            transactionNumber: Int,
            blockId: BlockId,
            offset: Int,
            value: Int
        ): Int {
            val transactionPos = Integer.BYTES
            val filePos = transactionPos + Integer.BYTES
            val blockPos = filePos + Page.maxLength(blockId.filename.length)
            val offsetPos = blockPos + Integer.BYTES
            val valuePos = offsetPos + Integer.BYTES
            val record = ByteArray(valuePos + Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.SET_INT.id)
            page.setInt(transactionPos, transactionNumber)
            page.setString(filePos, blockId.filename)
            page.setInt(blockPos, blockId.number)
            page.setInt(offsetPos, offset)
            page.setInt(valuePos, value)
            return logManager.append(record)
        }
    }
}
