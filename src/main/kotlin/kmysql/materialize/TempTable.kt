package kmysql.materialize

import kmysql.query.UpdateScan
import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.record.TableScan
import kmysql.transaction.Transaction
import java.util.concurrent.locks.ReentrantLock

class TempTable(
    private val transaction: Transaction,
    private val schema: Schema,
) {
    val tableName = nextTableName()
    val layout = Layout(schema)

    fun open(): UpdateScan {
        return TableScan(transaction, tableName, layout)
    }

    companion object {
        private var nextTableNum: Int = 0
        private val lock = ReentrantLock()

        fun nextTableName(): String {
            lock.lock()
            try {
                nextTableNum += 1
                return "temp$nextTableNum"
            } finally {
                lock.unlock()
            }
        }
    }
}
