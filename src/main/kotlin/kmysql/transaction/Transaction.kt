package kmysql.transaction

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.transaction.concurrency.ConcurrencyManager
import kmysql.transaction.recovery.RecoveryManager
import kmysql.util.ConsoleLogger
import java.util.concurrent.atomic.AtomicInteger

class Transaction(
    val fm: FileManager,
    val bm: BufferManager,
    val lm: LogManager,
) {
    private val endOfFile = -1
    private val rm: RecoveryManager
    private val cm: ConcurrencyManager
    private val transactionNumber: Int = nextTransactionNumber()
    private val buffers: BufferList

    init {
        rm = RecoveryManager(this, transactionNumber, lm, bm)
        cm = ConcurrencyManager()
        buffers = BufferList(bm)
    }

    fun commit() {
        rm.commit()
        cm.release()
        buffers.unpinAll()
        ConsoleLogger.info("transaction $transactionNumber committed")
    }

    fun rollback() {
        rm.rollback()
        cm.release()
        buffers.unpinAll()
        ConsoleLogger.info("transaction $transactionNumber rolled back")
    }

    fun recover() {
        bm.flushAll(transactionNumber)
        rm.recover()
    }

    fun pin(blockId: BlockId) {
        buffers.pin(blockId)
    }

    fun unpin(blockId: BlockId) {
        buffers.unpin(blockId)
    }

    fun getInt(blockId: BlockId, offset: Int): Int? {
        cm.sLock(blockId)
        val buffer = buffers.getBuffer(blockId) ?: return null
        return buffer.contents().getInt(offset)
    }

    fun getString(blockId: BlockId, offset: Int): String? {
        cm.sLock(blockId)
        val buffer = buffers.getBuffer(blockId) ?: return null
        return buffer.contents().getString(offset)
    }

    fun setInt(blockId: BlockId, offset: Int, value: Int, okToLog: Boolean) {
        cm.xLock(blockId)
        val buffer = buffers.getBuffer(blockId) ?: return
        val lsn = if (okToLog) rm.setInt(buffer, offset) else -1
        val page = buffer.contents()
        page.setInt(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    fun setString(blockId: BlockId, offset: Int, value: String, okToLog: Boolean) {
        cm.xLock(blockId)
        val buffer = buffers.getBuffer(blockId) ?: return
        val lsn = if (okToLog) rm.setString(buffer, offset) else -1
        val page = buffer.contents()
        page.setString(offset, value)
        buffer.setModified(transactionNumber, lsn)
    }

    fun size(filename: String): Int {
        val dummyBlock = BlockId(filename, endOfFile)
        cm.sLock(dummyBlock)
        return fm.length(filename)
    }

    fun append(filename: String): BlockId {
        val dummyBlock = BlockId(filename, endOfFile)
        cm.xLock(dummyBlock)
        return fm.append(filename)
    }

    fun blockSize(): Int = fm.blockSize

    fun availableBuffers(): Int = bm.available()

    companion object {
        private val nextTransactionNumber = AtomicInteger(0)

        fun nextTransactionNumber(): Int {
            val transactionNum = nextTransactionNumber.incrementAndGet()
            ConsoleLogger.info("new transaction $transactionNum")
            return transactionNum
        }
    }
}
