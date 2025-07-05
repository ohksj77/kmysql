package kmysql.transaction.recovery

import LogManager
import kmysql.file.Page
import kmysql.transaction.Transaction

class StartRecord(val page: Page) : LogRecord {
    private var transactionId: Long

    init {
        val transactionPosition = java.lang.Long.BYTES
        transactionId = page.getLong(transactionPosition)
    }

    override fun op(): Int {
        return Operator.START.id
    }

    override fun transactionId(): Long {
        return transactionId
    }

    override fun undo(transaction: Transaction) {}

    override fun toString(): String {
        return "<START $transactionId>"
    }

    companion object {
        fun writeToLog(logManager: LogManager, transactionId: Long): Int {
            val record = ByteArray(java.lang.Long.BYTES * 2)
            val page = Page(record)
            page.setInt(0, Operator.START.id)
            page.setLong(java.lang.Long.BYTES, transactionId)
            return logManager.append(record)
        }
    }
}
