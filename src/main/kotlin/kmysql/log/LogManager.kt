import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.file.Page
import kmysql.log.LogIterator
import kmysql.log.ReverseLogIterator
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

class LogManager(
    private val fm: FileManager,
    private val logfile: String,
) {
    private val lock = ReentrantLock()
    var logPage: Page
    var currentBlock: BlockId
    private var latestLSN = 0
    private var lastSavedLSN = 0

    init {
        val b = ByteArray(fm.blockSize)
        logPage = Page(b)

        try {
            val logSize = fm.length(logfile)
            if (logSize == 0) {
                currentBlock = appendNewBlock()
            } else {
                currentBlock = BlockId(logfile, logSize - 1)
                fm.read(currentBlock, logPage)
            }
        } catch (e: RuntimeException) {
            currentBlock = appendNewBlock()
        }
    }

    fun flush(lsn: Int) = lock.withLock {
        if (lsn >= lastSavedLSN) {
            flushInternal()
        }
    }

    fun iterator(): Iterator<ByteArray> = lock.withLock {
        flushInternal()
        return LogIterator(fm, currentBlock)
    }

    fun reverseIterator(): Iterator<ByteArray> = lock.withLock {
        flushInternal()
        return ReverseLogIterator(fm, currentBlock)
    }

    fun append(logRecord: ByteArray): Int = lock.withLock {
        var boundary = logPage.getInt(0)
        val recordSize = logRecord.size
        val bytesNeeded = recordSize + Integer.BYTES
        if (boundary - bytesNeeded < Integer.BYTES) {
            flushInternal()
            currentBlock = appendNewBlock()
            boundary = logPage.getInt(0)
        }
        val recordPosition = boundary - bytesNeeded
        logPage.setBytes(recordPosition, logRecord)
        logPage.setInt(0, recordPosition)
        latestLSN += 1

        flushInternal()

        return latestLSN
    }

    private fun appendNewBlock(): BlockId {
        val block = fm.append(logfile)
        logPage.setInt(0, fm.blockSize)
        fm.write(block, logPage)
        return block
    }

    private fun flushInternal() {
        fm.write(currentBlock, logPage)
        lastSavedLSN = latestLSN
    }
}
