package kmysql.transaction.recovery

import kmysql.file.Page
import kmysql.transaction.Transaction

enum class Operator(val id: Int) {
    CHECKPOINT(0),
    START(1),
    COMMIT(2),
    ROLLBACK(3),
    SET_INT(4),
    SET_STRING(5),
}

interface LogRecord {
    fun op(): Int

    fun transactionNumber(): Int

    fun undo(transaction: Transaction)

    companion object {
        fun createLogRecord(byteArray: ByteArray): LogRecord? {
            val p = Page(byteArray)
            when (p.getInt(0)) {
                Operator.CHECKPOINT.id -> CheckpointRecord
                Operator.START.id -> StartRecord(p)
                Operator.COMMIT.id -> CommitRecord(p)
                Operator.ROLLBACK.id -> RollbackRecord(p)
                Operator.SET_INT.id -> SetIntRecord(p)
                Operator.SET_STRING.id -> SetStringRecord(p)
            }
            return null
        }
    }
}
