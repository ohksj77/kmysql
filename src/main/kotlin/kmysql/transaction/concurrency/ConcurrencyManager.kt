package kmysql.transaction.concurrency

import kmysql.file.BlockId
import java.util.concurrent.ConcurrentHashMap

class ConcurrencyManager {
    private var locks = ConcurrentHashMap<BlockId, LockType>()

    companion object {
        private var lockTable = LockTable()
    }

    fun sLock(blockId: BlockId) {
        if (locks[blockId] == null) {
            lockTable.sLock(blockId)
            locks[blockId] = LockType.S
        }
    }

    fun xLock(blockId: BlockId) {
        if (!hasXLock(blockId)) {
            sLock(blockId)
            lockTable.xLock(blockId)
            locks[blockId] = LockType.X
        }
    }

    fun release() {
        for (blockId in locks.keys.toList()) {
            lockTable.unlock(blockId)
        }
        locks.clear()
    }

    private fun hasXLock(blockId: BlockId): Boolean {
        return locks[blockId] == LockType.X
    }

    enum class LockType { S, X }
}
