package kmysql.record

import kmysql.file.BlockId
import kmysql.transaction.Transaction

class RecordPage(
    private val transaction: Transaction,
    val blockId: BlockId,
    private val layout: Layout,
) {
    init {
        transaction.pin(blockId)
    }

    fun getInt(slot: Int, fieldName: String): Int {
        val layoutOffset = layout.offset(fieldName)
            ?: throw RecordPageException("Cannot get int value for field: $fieldName at slot: $slot")
        val fieldPosition = offset(slot) + layoutOffset
        val result = transaction.getInt(blockId, fieldPosition)
        if (result == null) {
            throw RecordPageException("Cannot get int value for field: $fieldName at slot: $slot")
        }
        return result
    }

    fun getString(slot: Int, fieldName: String): String {
        val layoutOffset = layout.offset(fieldName)
            ?: throw RecordPageException("Cannot get string value for field: $fieldName at slot: $slot")
        val fieldPosition = offset(slot) + layoutOffset
        val result = transaction.getString(blockId, fieldPosition)
        if (result == null) {
            throw RecordPageException("Cannot get string value for field: $fieldName at slot: $slot")
        }
        return result
    }

    fun setInt(slot: Int, fieldName: String, value: Int) {
        val layoutOffset = layout.offset(fieldName)
            ?: throw RecordPageException("Cannot get int value for field: $fieldName at slot: $slot")
        val fieldPosition = offset(slot) + layoutOffset
        transaction.setInt(blockId, fieldPosition, value, true)
    }

    fun setString(slot: Int, fieldName: String, value: String) {
        val layoutOffset = layout.offset(fieldName)
            ?: throw RecordPageException("Cannot get int value for field: $fieldName at slot: $slot")
        val fieldPosition = offset(slot) + layoutOffset
        val cleanValue = value.trim()
        transaction.setString(blockId, fieldPosition, cleanValue)
    }

    fun delete(slot: Int) {
        setFlag(slot, RecordPageState.EMPTY.id)
    }

    fun format() {
        var slot = 0
        while (isValidSlot(slot)) {
            transaction.setInt(blockId, offset(slot), RecordPageState.EMPTY.id, false)
            val schema = layout.schema()
            for (fieldName in schema.fields) {
                val layoutOffset = layout.offset(fieldName)
                if (layoutOffset != null) {
                    val fieldPosition = offset(slot) + layoutOffset
                    if (schema.type(fieldName) == java.sql.Types.INTEGER) {
                        transaction.setInt(blockId, fieldPosition, 0, false)
                    } else {
                        transaction.setString(blockId, fieldPosition, "")
                    }
                }
            }
            slot++
        }
    }

    fun nextAfter(slot: Int): Int {
        return searchAfter(slot, RecordPageState.USED.id)
    }

    fun insertAfter(slot: Int): Int {
        val newSlot = searchAfter(slot, RecordPageState.EMPTY.id)
        if (newSlot >= 0) setFlag(newSlot, RecordPageState.USED.id)
        return newSlot
    }

    private fun searchAfter(slot: Int, flag: Int): Int {
        var nextSlot = slot + 1
        while (isValidSlot(nextSlot)) {
            val transactionInt = transaction.getInt(blockId, offset(nextSlot))
            if (transactionInt != null && transactionInt == flag) {
                return nextSlot
            }
            nextSlot++
        }
        return -1
    }

    private fun isValidSlot(slot: Int): Boolean {
        return offset(slot + 1) <= transaction.blockSize()
    }

    private fun setFlag(slot: Int, flag: Int) {
        transaction.setInt(blockId, offset(slot), flag, true)
    }

    private fun offset(slot: Int): Int {
        return slot * layout.slotSize()
    }
}
