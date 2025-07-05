package kmysql.transaction.concurrency

import kmysql.file.BlockId
import kmysql.util.ConsoleLogger
import java.util.concurrent.ConcurrentHashMap

class ConcurrencyManager(
    private val transactionId: Long,
    private val isolationLevel: IsolationLevel = IsolationLevel.REPEATABLE_READ
) {
    private var locks = ConcurrentHashMap<BlockId, LockType>()
    private var lockPhase = LockPhase.GROWING
    private val lockTable = LockTable.getInstance()

    private val gapLocks = ConcurrentHashMap<BlockId, GapLock>()
    private var lockTimeout = 1000L

    fun sLock(blockId: BlockId) {
        if (lockPhase == LockPhase.SHRINKING) {
            throw RuntimeException("Cannot acquire lock in shrinking phase")
        }

        if (locks[blockId] == null) {
            try {
                lockTable.sLock(blockId, transactionId)
                locks[blockId] = LockType.S
            } catch (e: Exception) {
                ConsoleLogger.warn("S-lock 획득 실패: $blockId - ${e.message}")
            }
        }
    }

    fun xLock(blockId: BlockId) {
        if (lockPhase == LockPhase.SHRINKING) {
            throw RuntimeException("Cannot acquire lock in shrinking phase")
        }

        if (!hasXLock(blockId)) {
            try {
                lockTable.xLock(blockId, transactionId)
                locks[blockId] = LockType.X
            } catch (e: Exception) {
                ConsoleLogger.warn("X-lock 획득 실패: $blockId - ${e.message}")
            }
        }
    }

    fun nextKeyLock(blockId: BlockId) {
        if (lockPhase == LockPhase.SHRINKING) {
            throw RuntimeException("Cannot acquire lock in shrinking phase")
        }

        if (locks[blockId] == null) {
            lockTable.sLock(blockId, transactionId)
            locks[blockId] = LockType.S

            val gapLock = GapLock(
                blockId = blockId,
                transactionId = transactionId,
                lockType = GapLockType.NEXT_KEY
            )
            gapLocks[blockId] = gapLock
        }
    }

    fun gapLock(blockId: BlockId, gapType: GapLockType = GapLockType.GAP) {
        if (lockPhase == LockPhase.SHRINKING) {
            throw RuntimeException("Cannot acquire gap lock in shrinking phase")
        }

        val gapLock = GapLock(
            blockId = blockId,
            transactionId = transactionId,
            lockType = gapType
        )
        gapLocks[blockId] = gapLock
    }

    fun commit() {
        lockPhase = LockPhase.SHRINKING
        release()
    }

    fun rollback() {
        lockPhase = LockPhase.SHRINKING
        release()
    }

    fun release() {
        locks.forEach { (blockId, lockType) ->
            when (lockType) {
                LockType.S -> lockTable.sUnlock(blockId, transactionId)
                LockType.X -> lockTable.xUnlock(blockId, transactionId)
            }
        }
        locks.clear()
        gapLocks.clear()
        lockTable.clearTransaction(transactionId)
    }

    private fun hasXLock(blockId: BlockId): Boolean {
        return locks[blockId] == LockType.X
    }

    fun getIsolationLevel(): IsolationLevel = isolationLevel

    fun setLockTimeout(timeout: Long) {
        this.lockTimeout = timeout
    }

    fun getLockTimeout(): Long = lockTimeout

    fun getLockCount(): Int = locks.size

    fun hasLock(blockId: BlockId): Boolean = locks.containsKey(blockId) || gapLocks.containsKey(blockId)

    fun getLockType(blockId: BlockId): LockType? = locks[blockId]

    fun getGapLock(blockId: BlockId): GapLock? = gapLocks[blockId]

    fun getLockPhase(): LockPhase = lockPhase

    enum class LockType { S, X }
    enum class LockPhase { GROWING, SHRINKING }
}

data class GapLock(
    val blockId: BlockId,
    val transactionId: Long,
    val lockType: GapLockType,
    val timestamp: Long = System.currentTimeMillis()
)

enum class GapLockType {
    GAP,
    NEXT_KEY,
    INSERT_INTENTION
}
