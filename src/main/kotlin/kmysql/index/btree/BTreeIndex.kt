package kmysql.index.btree

import kmysql.file.BlockId
import kmysql.index.Index
import kmysql.query.Constant
import kmysql.record.Layout
import kmysql.record.Rid
import kmysql.record.Schema
import kmysql.transaction.Transaction
import kotlin.math.ln

/**
 * B Treeの操作を行うクラス
 */
class BTreeIndex(
    private val transaction: Transaction,
    private val indexName: String,
    private val leafLayout: Layout,
) : Index {
    private var leafTable = ""
    private var dirLayout: Layout
    private var rootBlockId: BlockId
    private var leaf: BTreeLeaf? = null

    init {
        leafTable = "${indexName}leaf"
        if (transaction.size(leafTable) == 0) {
            val blockId = transaction.append(leafTable)
            val leafNode = BTPage(transaction, blockId, leafLayout)
            leafNode.format(blockId, -1)
        }
        val dirSchema = Schema()
        dirSchema.add("block", leafLayout.schema())
        dirSchema.add("dataval", leafLayout.schema())
        val dirTable = "${indexName}dir"
        dirLayout = Layout(dirSchema)
        rootBlockId = BlockId(dirTable, 0)
        if (transaction.size(dirTable) == 0) {
            transaction.append(dirTable)
            val dirNode = BTPage(transaction, rootBlockId, dirLayout)
            dirNode.format(rootBlockId, 0)
            val fieldType = dirSchema.type("dataval")
            val minVal = if (fieldType == java.sql.Types.INTEGER) {
                Constant(Integer.MIN_VALUE)
            } else {
                Constant("")
            }
            dirNode.insertDir(0, minVal, 0)
            dirNode.close()
        }
    }

    override fun beforeFirst(searchKey: Constant) {
        close()
        val root = BTreeDir(transaction, dirLayout, rootBlockId)
        val blockIdNumber = root.search(searchKey)
        root.close()
        val leafBlockId = BlockId(leafTable, blockIdNumber)
        leaf = BTreeLeaf(transaction, leafLayout, searchKey, leafBlockId)
    }

    override fun next(): Boolean {
        return leaf!!.next()
    }

    override fun getDataRid(): Rid {
        return leaf!!.getDataRid()
    }

    override fun insert(dataValue: Constant, dataRid: Rid) {
        beforeFirst(dataValue)
        val entry = leaf?.insert(dataRid)
        leaf?.close()
        if (entry == null) {
            return
        }
        val root = BTreeDir(transaction, dirLayout, rootBlockId)
        val entry2 = root.insert(entry)
        if (entry2 != null) {
            root.makeNewRoot(entry2)
        }
        root.close()
    }

    override fun delete(dataValue: Constant, dataRid: Rid) {
        beforeFirst(dataValue)
        leaf?.delete(dataRid)
        leaf?.close()
    }

    override fun close() {
        if (leaf != null) {
            leaf?.close()
        }
    }

    companion object {
        fun searchCost(numberBlocks: Int, rpb: Int): Int {
            return 1 + (ln(numberBlocks.toDouble()) / ln(rpb.toDouble())).toInt()
        }
    }
}
