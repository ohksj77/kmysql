package kmysql.materialize

import kmysql.plan.Plan
import kmysql.query.Scan
import kmysql.query.UpdateScan
import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.transaction.Transaction
import kotlin.math.ceil

class MaterializePlan(
    private val srcPlan: Plan,
    private val transaction: Transaction,
) : Plan {
    override fun open(): Scan {
        val schema = srcPlan.schema()
        val tempTable = TempTable(transaction, schema)
        val srcScan: Scan = srcPlan.open()
        val destScan: UpdateScan = tempTable.open()
        while (srcScan.next()) {
            destScan.insert()
            for (fieldName in schema.fields) {
                destScan.setVal(fieldName, srcScan.getVal(fieldName))
            }
        }
        srcScan.close()
        destScan.beforeFirst()
        return destScan
    }

    override fun blocksAccessed(): Int {
        val dummyLayout = Layout(srcPlan.schema())
        val rpb = (transaction.blockSize() / dummyLayout.slotSize()).toDouble()
        return ceil(srcPlan.recordsOutput() / rpb).toInt()
    }

    override fun recordsOutput(): Int {
        return srcPlan.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return srcPlan.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return srcPlan.schema()
    }
}
