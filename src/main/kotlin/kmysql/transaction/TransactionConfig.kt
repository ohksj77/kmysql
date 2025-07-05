package kmysql.transaction

import kmysql.transaction.concurrency.IsolationLevel

data class TransactionConfig(
    val isolationLevel: IsolationLevel = IsolationLevel.REPEATABLE_READ,
    val autoCommit: Boolean = true,
    val lockTimeout: Long = 50L,
    val deadlockRetryCount: Int = 3,
    val enableNextKeyLocking: Boolean = true,
    val enableGapLocking: Boolean = true,
    val enableInsertIntentionLocking: Boolean = true,
    val maxSavePoints: Int = 100,
    val enableConsistentRead: Boolean = true,
    val enableMultiVersioning: Boolean = true
) {
    companion object {
        fun kmysqlDefault(): TransactionConfig = TransactionConfig()

        fun highPerformance(): TransactionConfig = TransactionConfig(
            isolationLevel = IsolationLevel.READ_COMMITTED,
            autoCommit = true,
            enableNextKeyLocking = false,
            enableGapLocking = false
        )

        fun highConsistency(): TransactionConfig = TransactionConfig(
            isolationLevel = IsolationLevel.SERIALIZABLE,
            autoCommit = false,
            lockTimeout = 100L
        )

        fun development(): TransactionConfig = TransactionConfig(
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
            autoCommit = true,
            lockTimeout = 10L,
            deadlockRetryCount = 1
        )
    }

    fun validate(): Boolean {
        return lockTimeout > 0 &&
                deadlockRetryCount >= 0 &&
                maxSavePoints > 0
    }

    fun toMySQLStyleString(): String {
        return """
            |Transaction Configuration:
            |  Isolation Level: ${isolationLevel.description}
            |  Auto Commit: $autoCommit
            |  Lock Timeout: ${lockTimeout}s
            |  Deadlock Retry Count: $deadlockRetryCount
            |  Next-Key Locking: $enableNextKeyLocking
            |  Gap Locking: $enableGapLocking
            |  Insert Intention Locking: $enableInsertIntentionLocking
            |  Max Savepoints: $maxSavePoints
            |  Consistent Read: $enableConsistentRead
            |  Multi-Versioning: $enableMultiVersioning
        """.trimMargin()
    }
} 
