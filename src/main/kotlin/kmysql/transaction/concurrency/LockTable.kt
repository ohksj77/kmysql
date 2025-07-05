package kmysql.transaction.concurrency

import kmysql.file.BlockId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock

class LockTable {
    private val maxWaitTime = 10_000L
    private val locks = ConcurrentHashMap<BlockId, LockInfo>()

    fun sLock(blockId: BlockId) = withLock(blockId) { info ->
        waitUntil(info, { info.lockCount < 0 }) { throw LockAbortException() }
        info.lockCount += 1
    }

    fun xLock(blockId: BlockId) = withLock(blockId) { info ->
        waitUntil(info, { info.lockCount > 0 }) { throw LockAbortException() }
        info.lockCount = -1
    }

    fun unlock(blockId: BlockId) {
        val info = locks[blockId] ?: return
        info.lock.lock()
        try {
            when {
                info.lockCount > 1 -> info.lockCount -= 1
                else -> {
                    locks.remove(blockId)
                    info.condition.signalAll()
                }
            }
        } finally {
            info.lock.unlock()
        }
    }

    private fun withLock(blockId: BlockId, action: (LockInfo) -> Unit) {
        val info = locks.computeIfAbsent(blockId) { LockInfo() }
        info.lock.lock()
        try {
            action(info)
        } finally {
            info.lock.unlock()
        }
    }

    private fun waitUntil(info: LockInfo, conditionCheck: () -> Boolean, onTimeout: () -> Unit) {
        val start = System.currentTimeMillis()
        while (conditionCheck() && !waitedTooLong(start)) {
            val remaining = maxWaitTime - (System.currentTimeMillis() - start)
            if (remaining <= 0) break
            info.condition.awaitNanos(remaining * 1_000_000)
        }
        if (conditionCheck()) onTimeout()
    }

    private fun waitedTooLong(start: Long) = System.currentTimeMillis() - start > maxWaitTime

    private class LockInfo {
        val lock = ReentrantLock()
        val condition: Condition = lock.newCondition()
        var lockCount: Int = 0
    }
}
