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
    private var transactionNum: Int? = null
    private var lsn: Int? = null

    fun contents(): Page = contents

    fun blockId(): BlockId? = blockId

    fun setModified(newTransactionNum: Int, newLsn: Int) {
        transactionNum = newTransactionNum
        lsn = newLsn
    }

    fun isPinned(): Boolean = pins > 0

    fun modifyingTransaction(): Int? = transactionNum

    fun assignToBlock(b: BlockId) {
        flush()
        blockId = b
        fm.read(b, contents)
        pins = 0
        transactionNum = null
        lsn = null
    }

    fun flush() {
        if (transactionNum != null && blockId != null && lsn != null) {
            lm.flush(lsn!!)
            fm.write(blockId!!, contents)
            transactionNum = null
            lsn = null
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
