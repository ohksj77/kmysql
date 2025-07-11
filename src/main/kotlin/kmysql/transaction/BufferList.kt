package kmysql.transaction

import kmysql.buffer.Buffer
import kmysql.buffer.BufferManager
import kmysql.file.BlockId

class BufferList(private val bufferManager: BufferManager) {
    private val buffers = mutableMapOf<BlockId, Buffer>()
    private val pins = mutableListOf<BlockId>()

    fun getBuffer(blockId: BlockId): Buffer? {
        return buffers[blockId]
    }

    fun pin(blockId: BlockId): Buffer {
        val buffer = bufferManager.pin(blockId)
        buffers[blockId] = buffer
        pins.add(blockId)
        return buffer
    }

    fun unpin(blockId: BlockId) {
        val buffer = buffers[blockId]
        if (buffer != null) {
            bufferManager.unpin(buffer)
            pins.remove(blockId)
            if (!pins.contains(blockId)) buffers.remove(blockId)
        }
    }

    fun unpinAll() {
        for (blockId in pins) {
            val buffer = buffers[blockId]
            if (buffer != null) bufferManager.unpin(buffer)
        }
        buffers.clear()
        pins.clear()
    }

    fun getAllBlockIds(): List<BlockId> {
        return buffers.keys.toList()
    }
}
