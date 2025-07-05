package kmysql.index.query

import kmysql.index.Index
import kmysql.query.Constant
import kmysql.query.Scan
import kmysql.record.TableScan

class IndexSelectScan(
    private val tableScan: TableScan,
    private val index: Index,
    private val value: Constant,
) : Scan {
    
    init {
        beforeFirst()
    }

    override fun beforeFirst() {
        index.beforeFirst(value)
    }

    override fun next(): Boolean {
        val ok = index.next()
        if (ok) {
            val rid = index.getDataRid()
            tableScan.moveToRid(rid)
        }
        return ok
    }

    override fun getInt(fieldName: String): Int {
        return tableScan.getInt(fieldName)
    }

    override fun getString(fieldName: String): String {
        return tableScan.getString(fieldName)
    }

    override fun getVal(fieldName: String): Constant {
        return tableScan.getVal(fieldName)
    }

    override fun hasField(fieldName: String): Boolean {
        return tableScan.hasField(fieldName)
    }

    override fun close() {
        index.close()
        tableScan.close()
    }
}
