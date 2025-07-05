package kmysql.buffer

import LogManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import java.util.concurrent.locks.ReentrantLock

class BufferManager(
    val fm: FileManager,
    val lm: LogManager,
    var numBuffers: Int,
) {
    private val bufferPool: MutableList<Buffer> = MutableList(numBuffers) { Buffer(fm, lm) }
    private var numAvailable = numBuffers
    private val lock = ReentrantLock()
    private val condition = lock.newCondition()

    fun available(): Int = withLock { numAvailable }

    fun flushAll(transactionNum: Int) = withLock {
        bufferPool.forEach { buffer ->
            if (buffer.modifyingTransaction() == transactionNum) {
                buffer.flush()
            }
        }
    }

    fun unpin(buffer: Buffer) = withLock {
        buffer.unpin()
        if (!buffer.isPinned()) {
            numAvailable++
            condition.signalAll()
        }
    }

    fun pin(blockId: BlockId): Buffer = withLock {
        val timestamp = System.currentTimeMillis()
        var buffer = tryToPin(blockId)
        while (buffer == null && !waitingTooLong(timestamp)) {
            condition.awaitNanos(MAX_TIME * 1_000_000)
            buffer = tryToPin(blockId)
        }
        if (buffer == null) {
            throw BufferAbortException.timeout()
        }
        buffer
    }

    private fun waitingTooLong(startTime: Long): Boolean =
        System.currentTimeMillis() - startTime > MAX_TIME

    private fun tryToPin(blockId: BlockId): Buffer? {
        var buffer = findExistingBuffer(blockId)
        if (buffer == null) {
            buffer = chooseUnpinnedBuffer()
            if (buffer == null) return null
            buffer.assignToBlock(blockId)
        }
        if (!buffer.isPinned()) {
            numAvailable--
        }
        buffer.pin()
        return buffer
    }

    private fun findExistingBuffer(blockId: BlockId): Buffer? =
        bufferPool.firstOrNull { it.blockId() == blockId }

    private fun chooseUnpinnedBuffer(): Buffer? =
        bufferPool.firstOrNull { !it.isPinned() }

    private inline fun <T> withLock(action: () -> T): T {
        lock.lock()
        return try {
            action()
        } finally {
            lock.unlock()
        }
    }

    companion object {
        private const val MAX_TIME = 1000L
    }
}
