package kmysql.log

import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.file.Page
import kmysql.util.ConsoleLogger

class ReverseLogIterator(val fm: FileManager, var blockId: BlockId) : Iterator<ByteArray> {
    private var page: Page
    private var currentPosition = 0
    private var boundary = 0
    private var recordsInCurrentBlock = mutableListOf<ByteArray>()
    private var currentRecordIndex = 0

    init {
        val b = ByteArray(fm.blockSize)
        page = Page(b)
        moveToBlock(blockId)
        loadRecordsFromCurrentBlock()
    }

    override fun hasNext(): Boolean {
        return currentRecordIndex < recordsInCurrentBlock.size || blockId.number > 0
    }

    override fun next(): ByteArray {
        if (currentRecordIndex >= recordsInCurrentBlock.size) {
            blockId = BlockId(blockId.filename, blockId.number - 1)
            moveToBlock(blockId)
            loadRecordsFromCurrentBlock()
        }

        return recordsInCurrentBlock[currentRecordIndex++]
    }

    private fun moveToBlock(blk: BlockId) {
        fm.read(blk, page)
        boundary = page.getInt(0)
        currentPosition = boundary
    }

    private fun loadRecordsFromCurrentBlock() {
        recordsInCurrentBlock.clear()
        currentRecordIndex = 0

        ConsoleLogger.info("ReverseLogIterator.loadRecordsFromCurrentBlock: blockId=$blockId, boundary=$boundary, blockSize=${fm.blockSize}")

        var pos = boundary
        while (pos < fm.blockSize) {
            try {
                val record = page.getBytes(pos)
                recordsInCurrentBlock.add(record)
                ConsoleLogger.info("ReverseLogIterator: found record at pos=$pos, size=${record.size}")
                pos += Integer.BYTES + record.size
            } catch (e: Exception) {
                ConsoleLogger.info("ReverseLogIterator: exception at pos=$pos: ${e.message}")
                break
            }
        }

        ConsoleLogger.info("ReverseLogIterator: loaded ${recordsInCurrentBlock.size} records")
    }
} 
