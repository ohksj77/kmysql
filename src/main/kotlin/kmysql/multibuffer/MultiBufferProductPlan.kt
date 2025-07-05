package kmysql.multibuffer

import kmysql.materialize.MaterializePlan
import kmysql.materialize.TempTable
import kmysql.plan.Plan
import kmysql.query.Scan
import kmysql.record.Schema
import kmysql.transaction.Transaction

class MultiBufferProductPlan(
    private val transaction: Transaction,
    private var leftHandSidePlan: Plan,
    private var rightHandSidePlan: Plan,
) : Plan {
    private val schema = Schema()

    init {
        leftHandSidePlan = MaterializePlan(leftHandSidePlan, transaction)
        schema.addAll(leftHandSidePlan.schema())
        schema.addAll(rightHandSidePlan.schema())
    }

    override fun open(): Scan {
        val leftHandSideScan = leftHandSidePlan.open()
        val tempTable = copyRecordsFrom(rightHandSidePlan)
        return MultiBufferProductScan(transaction, leftHandSideScan, tempTable.tableName, tempTable.layout)
    }

    override fun blocksAccessed(): Int {
        val avail = transaction.availableBuffers()
        val size = MaterializePlan(rightHandSidePlan, transaction).blocksAccessed()
        val numChunks = size / avail
        return rightHandSidePlan.blocksAccessed() + (leftHandSidePlan.blocksAccessed() + numChunks)
    }

    override fun recordsOutput(): Int {
        return leftHandSidePlan.recordsOutput() * rightHandSidePlan.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return if (leftHandSidePlan.schema().hasField(fieldName)) {
            leftHandSidePlan.distinctValues(fieldName)
        } else {
            rightHandSidePlan.distinctValues(fieldName)
        }
    }

    override fun schema(): Schema {
        return schema
    }

    private fun copyRecordsFrom(plan: Plan): TempTable {
        val srcPlan = plan.open()
        val sch = plan.schema()
        val tempTable = TempTable(transaction, sch)
        val destScan = tempTable.open()
        while (srcPlan.next()) {
            destScan.insert()
            for (fieldName in sch.fields) {
                destScan.setVal(fieldName, srcPlan.getVal(fieldName))
            }
        }
        srcPlan.close()
        destScan.close()
        return tempTable
    }
}
