package kmysql.transaction.recovery

import kmysql.file.Page
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

enum class Operator(val id: Int) {
    CHECKPOINT(0),
    START(1),
    COMMIT(2),
    ROLLBACK(3),
    SET_INT(4),
    SET_STRING(5),
    SAVEPOINT(6),
    SAVEPOINT_ROLLBACK(7),
    RELEASE_SAVEPOINT(8),
}

interface LogRecord {
    fun op(): Int

    fun transactionId(): Long

    fun undo(transaction: Transaction)

    companion object {
        fun createLogRecord(byteArray: ByteArray): LogRecord? {
            val p = Page(byteArray)
            val op = p.getInt(0)
            ConsoleLogger.info("LogRecord.createLogRecord: op=$op, byteArray.size=${byteArray.size}")
            
            return try {
                when (op) {
                Operator.CHECKPOINT.id -> CheckpointRecord
                Operator.START.id -> StartRecord(p)
                Operator.COMMIT.id -> CommitRecord(p)
                Operator.ROLLBACK.id -> RollbackRecord(p)
                Operator.SET_INT.id -> SetIntRecord(p)
                Operator.SET_STRING.id -> SetStringRecord(p)
                Operator.SAVEPOINT.id -> SavepointRecord(p)
                Operator.SAVEPOINT_ROLLBACK.id -> SavepointRollbackRecord(p)
                Operator.RELEASE_SAVEPOINT.id -> ReleaseSavepointRecord(p)
                    else -> {
                        ConsoleLogger.info("LogRecord.createLogRecord: unknown operator $op")
                        null
                    }
                }
            } catch (e: Exception) {
                ConsoleLogger.error("LogRecord.createLogRecord: exception creating record for op=$op: ${e.message}")
                null
            }
        }
    }
}
