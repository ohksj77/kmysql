package kmysql.transaction.recovery

import LogManager
import kmysql.file.Page
import kmysql.transaction.Transaction

object CheckpointRecord : LogRecord {
    override fun op(): Int = Operator.CHECKPOINT.id

    override fun transactionId(): Long = -1

    override fun undo(transaction: Transaction) {
    }

    override fun toString(): String = "<CHECKPOINT>"

    fun writeToLog(lm: LogManager): Int {
        val record = ByteArray(Integer.BYTES)
        val page = Page(record)
        page.setInt(0, Operator.CHECKPOINT.id)
        return lm.append(record)
    }
}
