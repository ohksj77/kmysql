package kmysql.record

import kmysql.file.BlockId
import kmysql.query.Constant
import kmysql.query.UpdateScan
import kmysql.transaction.Transaction

class TableScan(
    private val transaction: Transaction,
    private val tableName: String,
    private val layout: Layout,
) : UpdateScan {
    private var recordPage: RecordPage? = null
    private var fileName: String = ""
    private var currentSlot: Int = 0

    init {
        val isSystemTable = tableName in listOf("tablecatalog", "fieldcatalog", "indexcatalog", "viewcatalog")
        fileName = if (isSystemTable) {
            tableName
        } else if (tableName.endsWith(".tbl")) {
            if (tableName in listOf("tablecatalog.tbl", "fieldcatalog.tbl", "indexcatalog.tbl", "viewcatalog.tbl")) {
                tableName.substring(0, tableName.length - 4)
            } else {
                tableName
            }
        } else {
            "$tableName.tbl"
        }

        if (fileName.endsWith(".tbl.tbl")) {
            fileName = fileName.removeSuffix(".tbl.tbl") + ".tbl"
        }

        val fileSize = try {
            transaction.size(fileName)
        } catch (e: Exception) {
            0
        }

        if (fileSize == 0) {
            moveToNewBlock()
        } else {
            moveToBlock(0)
        }
    }

    override fun close() {
        if (recordPage != null) {
            transaction.unpin(recordPage!!.blockId)
        }
    }

    override fun beforeFirst() {
        moveToBlock(0)
    }

    override fun next(): Boolean {
        currentSlot = recordPage!!.nextAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) return false
            moveToBlock(recordPage!!.blockId.number + 1)
            currentSlot = recordPage!!.nextAfter(currentSlot)
        }
        return true
    }

    override fun getInt(fieldName: String): Int {
        return recordPage!!.getInt(currentSlot, fieldName)
    }

    override fun getString(fieldName: String): String {
        val result = recordPage!!.getString(currentSlot, fieldName)
        return result
    }

    override fun getVal(fieldName: String): Constant {
        return if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
            Constant(getInt(fieldName))
        } else {
            Constant(getString(fieldName))
        }
    }

    override fun hasField(fieldName: String): Boolean {
        return layout.schema().hasField(fieldName)
    }

    override fun setInt(fieldName: String, value: Int) {
        recordPage!!.setInt(currentSlot, fieldName, value)
    }

    override fun setString(fieldName: String, value: String) {
        recordPage!!.setString(currentSlot, fieldName, value)
    }

    override fun setVal(fieldName: String, value: Constant) {
        if (layout.schema().type(fieldName) == java.sql.Types.INTEGER) {
            val intValue = value.asInt() ?: throw RuntimeException("null value")
            setInt(fieldName, intValue)
        } else {
            val stringValue = value.asString() ?: throw RuntimeException("null value")
            setString(fieldName, stringValue)
        }
    }

    override fun insert() {
        currentSlot = recordPage!!.insertAfter(currentSlot)
        while (currentSlot < 0) {
            if (atLastBlock()) {
                moveToNewBlock()
            } else {
                moveToBlock(recordPage!!.blockId.number + 1)
            }
            currentSlot = recordPage!!.insertAfter(currentSlot)
        }
    }

    override fun delete() {
        recordPage!!.delete(currentSlot)
    }

    override fun moveToRid(rid: Rid) {
        close()
        val blockId = BlockId(fileName, rid.blockNumber)
        recordPage = RecordPage(transaction, blockId, layout)
        currentSlot = rid.slot
    }

    override fun getRid(): Rid {
        return Rid(recordPage!!.blockId.number, currentSlot)
    }

    private fun moveToBlock(blockNumber: Int) {
        close()
        val blockId = BlockId(fileName, blockNumber)
        recordPage = RecordPage(transaction, blockId, layout)
        currentSlot = -1
    }

    private fun moveToNewBlock() {
        close()
        val blockId = transaction.append(fileName)
        recordPage = RecordPage(transaction, blockId, layout)
        recordPage!!.format()
        currentSlot = -1
    }

    private fun atLastBlock(): Boolean {
        return recordPage!!.blockId.number == (transaction.size(fileName) - 1)
    }
}
