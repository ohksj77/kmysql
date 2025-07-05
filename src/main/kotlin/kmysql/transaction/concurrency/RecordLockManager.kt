package kmysql.transaction.concurrency

import kmysql.file.BlockId
import kmysql.record.Rid
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock

class RecordLockManager {
    private val recordLocks = ConcurrentHashMap<Rid, ReentrantReadWriteLock>()
    
    companion object {
        @Volatile
        private var instance: RecordLockManager? = null
        
        fun getInstance(): RecordLockManager {
            return instance ?: synchronized(this) {
                instance ?: RecordLockManager().also { instance = it }
            }
        }
    }

    fun sLockRecord(rid: Rid) {
        val lock = recordLocks.computeIfAbsent(rid) { ReentrantReadWriteLock() }
        if (!lock.readLock().tryLock(5, TimeUnit.SECONDS)) {
            throw RuntimeException("Record sLock timeout: $rid")
        }
    }

    fun xLockRecord(rid: Rid) {
        val lock = recordLocks.computeIfAbsent(rid) { ReentrantReadWriteLock() }
        if (!lock.writeLock().tryLock(5, TimeUnit.SECONDS)) {
            throw RuntimeException("Record xLock timeout: $rid")
        }
    }

    fun sUnlockRecord(rid: Rid) {
        val lock = recordLocks[rid] ?: return
        try {
            lock.readLock().unlock()
        } catch (ignored: IllegalMonitorStateException) {
        }
    }

    fun xUnlockRecord(rid: Rid) {
        val lock = recordLocks[rid] ?: return
        try {
            lock.writeLock().unlock()
        } catch (ignored: IllegalMonitorStateException) {
        }
    }

    fun getLockCount(rid: Rid): Int {
        val lock = recordLocks[rid] ?: return 0
        return lock.readHoldCount + if (lock.isWriteLocked) 1 else 0
    }
} 