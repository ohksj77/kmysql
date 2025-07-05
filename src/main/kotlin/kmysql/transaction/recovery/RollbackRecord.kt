package kmysql.transaction.recovery

import LogManager
import kmysql.file.Page
import kmysql.transaction.Transaction

class RollbackRecord(private val page: Page) : LogRecord {
    val transactionNumber: Int = page.getInt(Integer.BYTES)

    override fun op(): Int = Operator.ROLLBACK.id

    override fun transactionNumber(): Int = transactionNumber

    override fun undo(transaction: Transaction) {
    }

    override fun toString(): String = "<ROLLBACK $transactionNumber>"

    companion object {
        fun writeToLog(lm: LogManager, transactionNumber: Int): Int {
            val record = ByteArray(2 * Integer.BYTES)
            val page = Page(record)
            page.setInt(0, Operator.ROLLBACK.id)
            page.setInt(Integer.BYTES, transactionNumber)
            return lm.append(record)
        }
    }
}
