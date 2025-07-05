package kmysql.buffer

import LogManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.file.Page

class Buffer(
    val fm: FileManager,
    val lm: LogManager,
) {
    private var contents: Page = Page(fm.blockSize)
    private var blockId: BlockId? = null
    private var pins = 0
    private var transactionId: Long? = null
    private var lsn: Int? = null

    fun contents(): Page = contents

    fun blockId(): BlockId? = blockId

    fun setModified(newtransactionId: Long, newLsn: Int) {
        transactionId = newtransactionId
        lsn = newLsn
    }

    fun isPinned(): Boolean = pins > 0

    fun modifyingTransaction(): Long? = transactionId

    fun assignToBlock(b: BlockId) {
        flush()
        blockId = b
        fm.read(b, contents)
        pins = 0
        transactionId = null
        lsn = null
    }

    fun flush() {
        if (transactionId != null && blockId != null && lsn != null) {
            lm.flush(lsn!!)
            fm.write(blockId!!, contents)
            transactionId = null
            lsn = null
        }
    }

    fun forceFlush() {
        if (blockId != null) {
            fm.write(blockId!!, contents)
        }
    }

    fun pin() {
        pins++
    }

    fun unpin() {
        if (pins > 0) {
            pins--
        }
    }
}
