package kmysql.index.btree

import kmysql.file.BlockId
import kmysql.query.Constant
import kmysql.record.Layout
import kmysql.record.Rid
import kmysql.transaction.Transaction

class BTreeLeaf(
    private val transaction: Transaction,
    private val layout: Layout,
    private val searchKey: Constant,
    private val blockId: BlockId,
) {
    private var contents: BTPage = BTPage(transaction, blockId, layout)
    private var currentSlot = contents.findSlotBefore(searchKey)
    private val filename = blockId.filename

    fun close() {
        contents.close()
    }

    fun next(): Boolean {
        currentSlot += 1
        return if (currentSlot >= contents.getNumRecords()) {
            tryOverflow()
        } else if (contents.getDataVal(currentSlot).equals(searchKey)) {
            true
        } else {
            tryOverflow()
        }
    }

    fun getDataRid(): Rid {
        return contents.getDataRid(currentSlot)
    }

    fun delete(dataRid: Rid) {
        while (next()) {
            if (getDataRid() == dataRid) {
                contents.delete(currentSlot)
                return
            }
        }
    }

    fun insert(dataRid: Rid): DirEntry? {
        if (contents.getFlag() >= 0 && contents.getDataVal(0).compareTo(searchKey) > 0) {
            val firstVal = contents.getDataVal(0)
            val newBlockInt = contents.split(0, contents.getFlag())
            currentSlot = 0
            contents.setFlag(-1)
            contents.insertLeaf(currentSlot, searchKey, dataRid)
            return DirEntry(firstVal, newBlockInt.number)
        }
        currentSlot += 1
        contents.insertLeaf(currentSlot, searchKey, dataRid)
        if (!contents.isFull()) {
            return null
        }
        val firstKey = contents.getDataVal(0)
        val lastKey = contents.getDataVal(contents.getNumRecords() - 1)
        if (lastKey.equals(firstKey)) {
            val newBlockId = contents.split(1, contents.getFlag())
            contents.setFlag(newBlockId.number)
            return null
        } else {
            var splitPosition = contents.getNumRecords() / 2
            var splitKey = contents.getDataVal(splitPosition)
            if (splitKey.equals(firstKey)) {
                while (contents.getDataVal(splitPosition).equals(splitPosition)) splitPosition += 1
                splitKey = contents.getDataVal(splitPosition)
            } else {
                while (contents.getDataVal(splitPosition - 1).equals(splitKey)) splitPosition -= 1
            }
            val newBlockId = contents.split(splitPosition, -1)
            return DirEntry(splitKey, newBlockId.number)
        }
    }

    private fun tryOverflow(): Boolean {
        val firstKey = contents.getDataVal(0)
        val flag = contents.getFlag()
        if (searchKey != firstKey || flag < 0) {
            return false
        }
        contents.close()
        val nextBlockId = BlockId(filename, flag)
        contents = BTPage(transaction, nextBlockId, layout)
        currentSlot = 0
        return true
    }
}
