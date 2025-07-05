package kmysql.transaction.recovery

import LogManager
import kmysql.file.Page
import kmysql.transaction.Transaction

class RollbackRecord(private val page: Page) : LogRecord {
    val transactionId: Long = page.getLong(java.lang.Long.BYTES)

    override fun op(): Int = Operator.ROLLBACK.id

    override fun transactionId(): Long = transactionId

    override fun undo(transaction: Transaction) {
    }

    override fun toString(): String = "<ROLLBACK $transactionId>"

    companion object {
        fun writeToLog(lm: LogManager, transactionId: Long): Int {
            val record = ByteArray(java.lang.Long.BYTES * 2)
            val page = Page(record)
            page.setInt(0, Operator.ROLLBACK.id)
            page.setLong(java.lang.Long.BYTES, transactionId)
            return lm.append(record)
        }
    }
}
