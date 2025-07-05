package kmysql.transaction.concurrency

import kmysql.file.BlockId
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class DeadlockDetector {
    private val waitForGraph = ConcurrentHashMap<Long, MutableSet<Long>>()
    private val lockHolders = ConcurrentHashMap<BlockId, Long>()
    private val lockWaiters = ConcurrentHashMap<BlockId, MutableSet<Long>>()
    private val transactionIdGenerator = AtomicLong(0)

    companion object {
        @Volatile
        private var instance: DeadlockDetector? = null

        fun getInstance(): DeadlockDetector {
            return instance ?: synchronized(this) {
                instance ?: DeadlockDetector().also { instance = it }
            }
        }
    }

    fun getNextTransactionId(): Long {
        val id = transactionIdGenerator.incrementAndGet()
        return id
    }

    fun addLockRequest(transactionId: Long, blockId: BlockId, isExclusive: Boolean) {
        val currentHolder = lockHolders[blockId]

        if (currentHolder == null) {
            lockHolders[blockId] = transactionId
        } else if (currentHolder != transactionId) {
            lockWaiters.computeIfAbsent(blockId) { mutableSetOf() }.add(transactionId)

            waitForGraph.computeIfAbsent(transactionId) { mutableSetOf() }.add(currentHolder)

            if (hasCycle(transactionId)) {
                val victim = selectVictim(transactionId, currentHolder)
                throw DeadlockException("Deadlock detected, transaction $victim will be rolled back")
            }
        }
    }

    fun removeLockRequest(transactionId: Long, blockId: BlockId) {
        lockHolders.remove(blockId)?.let { holder ->
            if (holder == transactionId) {
                val waiters = lockWaiters[blockId]
                if (waiters != null && waiters.isNotEmpty()) {
                    val nextWaiter = waiters.first()
                    waiters.remove(nextWaiter)
                    lockHolders[blockId] = nextWaiter

                    waitForGraph[transactionId]?.remove(nextWaiter)
                }
            }
        }

        waitForGraph.remove(transactionId)
        lockWaiters.values.forEach { it.remove(transactionId) }
    }

    private fun hasCycle(startTransactionId: Long): Boolean {
        val visited = mutableSetOf<Long>()
        val recursionStack = mutableSetOf<Long>()

        return dfs(startTransactionId, visited, recursionStack)
    }

    private fun dfs(transactionId: Long, visited: MutableSet<Long>, recursionStack: MutableSet<Long>): Boolean {
        if (recursionStack.contains(transactionId)) {
            return true
        }

        if (visited.contains(transactionId)) {
            return false
        }

        visited.add(transactionId)
        recursionStack.add(transactionId)

        val neighbors = waitForGraph[transactionId] ?: emptySet()
        for (neighbor in neighbors) {
            if (dfs(neighbor, visited, recursionStack)) {
                return true
            }
        }

        recursionStack.remove(transactionId)
        return false
    }

    private fun selectVictim(transaction1: Long, transaction2: Long): Long {
        return minOf(transaction1, transaction2)
    }

    fun clearTransaction(transactionId: Long) {
        waitForGraph.remove(transactionId)
        lockWaiters.values.forEach { it.remove(transactionId) }

        lockHolders.entries.removeIf { it.value == transactionId }
    }
}

class DeadlockException(message: String) : RuntimeException(message)
