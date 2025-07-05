package kmysql.transaction.recovery

import LogManager
import kmysql.file.Page
import kmysql.transaction.Transaction

class ReleaseSavepointRecord(private val page: Page) : LogRecord {
    val transactionId: Long = page.getLong(java.lang.Long.BYTES)
    val savepointName: String = page.getString(java.lang.Long.BYTES * 2)

    override fun op(): Int = Operator.RELEASE_SAVEPOINT.id

    override fun transactionId(): Long = transactionId

    override fun undo(transaction: Transaction) {
    }

    override fun toString(): String = "<RELEASE_SAVEPOINT $transactionId $savepointName>"

    companion object {
        fun writeToLog(lm: LogManager, transactionId: Long, savepointName: String): Int {
            val record = ByteArray(java.lang.Long.BYTES * 2 + Page.maxLength(savepointName.length))
            val page = Page(record)
            page.setInt(0, Operator.RELEASE_SAVEPOINT.id)
            page.setLong(java.lang.Long.BYTES, transactionId)
            page.setString(java.lang.Long.BYTES * 2, savepointName)
            return lm.append(record)
        }
    }
} 
