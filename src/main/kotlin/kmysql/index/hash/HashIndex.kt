package kmysql.index.hash

import kmysql.index.Index
import kmysql.query.Constant
import kmysql.record.Layout
import kmysql.record.Rid
import kmysql.record.TableScan
import kmysql.transaction.Transaction

class HashIndex(
    private val transaction: Transaction,
    private val indexName: String,
    private val layout: Layout,
) : Index {
    val numberBuckets = 100
    private var searchKey: Constant? = null
    private var tableScan: TableScan? = null

    override fun beforeFirst(searchKey: Constant) {
        close()
        this.searchKey = searchKey
        val bucket = searchKey.hashCode() % numberBuckets
        val tableName = "$indexName$bucket"
        tableScan = TableScan(transaction, tableName, layout)
    }

    override fun next(): Boolean {
        while (tableScan != null && tableScan!!.next()) {
            val dataValue = tableScan?.getVal("dataval") ?: throw RuntimeException("null error")
            if (dataValue == searchKey) {
                return true
            }
        }
        return false
    }

    override fun getDataRid(): Rid {
        val blockNumber = tableScan?.getInt("block") ?: throw RuntimeException("null error")
        val id = tableScan?.getInt("id") ?: throw RuntimeException("null error")
        return Rid(blockNumber, id)
    }

    override fun insert(dataValue: Constant, dataRid: Rid) {
        beforeFirst(dataValue)
        tableScan?.insert()
        tableScan?.setInt("block", dataRid.blockNumber)
        tableScan?.setInt("id", dataRid.slot)
        tableScan?.setVal("dataval", dataValue)
    }

    override fun delete(dataValue: Constant, dataRid: Rid) {
        beforeFirst(dataValue)
        while (next()) {
            if (getDataRid() == dataRid) {
                tableScan?.delete()
                return
            }
        }
    }

    override fun close() {
        if (tableScan != null) {
            tableScan?.close()
        }
    }

    companion object {
        private const val NUMBER_BUCKETS = 100

        fun searchCost(numberBlocks: Int, recordPerBlock: Int): Int {
            return numberBlocks / NUMBER_BUCKETS
        }
    }
}
