package kmysql.transaction.concurrency

import kmysql.file.BlockId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

class LockTable {
    private val locks = ConcurrentHashMap<BlockId, ReentrantReadWriteLock>()
    private val deadlockDetector = DeadlockDetector.getInstance()
    private val lockTimeout = 50L

    companion object {
        @Volatile
        private var instance: LockTable? = null

        fun getInstance(): LockTable {
            return instance ?: synchronized(this) {
                instance ?: LockTable().also { instance = it }
            }
        }
    }

    fun sLock(blockId: BlockId, transactionId: Long) {
        val lock = locks.computeIfAbsent(blockId) { ReentrantReadWriteLock() }

        deadlockDetector.addLockRequest(transactionId, blockId, false)

        try {
            if (!lock.readLock().tryLock(lockTimeout, TimeUnit.SECONDS)) {
                deadlockDetector.removeLockRequest(transactionId, blockId)
                throw RuntimeException("sLock timeout (possible deadlock): $blockId")
            }
        } catch (e: DeadlockException) {
            deadlockDetector.removeLockRequest(transactionId, blockId)
            throw e
        }
    }

    fun xLock(blockId: BlockId, transactionId: Long) {
        val lock = locks.computeIfAbsent(blockId) { ReentrantReadWriteLock() }

        deadlockDetector.addLockRequest(transactionId, blockId, true)

        try {
            val readLockCount = lock.readHoldCount
            if (readLockCount > 0) {
                repeat(readLockCount) {
                    lock.readLock().unlock()
                }
            }

            if (!lock.writeLock().tryLock(lockTimeout, TimeUnit.SECONDS)) {
                deadlockDetector.removeLockRequest(transactionId, blockId)
                throw RuntimeException("xLock timeout (possible deadlock): $blockId")
            }
        } catch (e: DeadlockException) {
            deadlockDetector.removeLockRequest(transactionId, blockId)
            throw e
        }
    }

    fun sUnlock(blockId: BlockId, transactionId: Long) {
        val lock = locks[blockId] ?: return
        try {
            lock.readLock().unlock()
            deadlockDetector.removeLockRequest(transactionId, blockId)
        } catch (ignored: IllegalMonitorStateException) {
        }
    }

    fun xUnlock(blockId: BlockId, transactionId: Long) {
        val lock = locks[blockId] ?: return
        try {
            lock.writeLock().unlock()
            deadlockDetector.removeLockRequest(transactionId, blockId)
        } catch (ignored: IllegalMonitorStateException) {
        }
    }

    fun clearTransaction(transactionId: Long) {
        deadlockDetector.clearTransaction(transactionId)
    }

    fun setLockTimeout(timeoutSeconds: Long) {
    }
}
