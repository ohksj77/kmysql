package kmysql.plan

import kmysql.query.Predicate
import kmysql.query.Scan
import kmysql.query.SelectScan
import kmysql.record.Schema

class SelectPlan(
    private val plan: Plan,
    private val predicate: Predicate,
) : Plan {

    override fun open(): Scan {
        val scan = plan.open()
        return SelectScan(scan, predicate)
    }

    override fun blocksAccessed(): Int {
        return plan.blocksAccessed()
    }

    override fun recordsOutput(): Int {
        return plan.recordsOutput() / predicate.reductionFactor(plan)
    }

    override fun distinctValues(fieldName: String): Int {
        return if (predicate.equateWithConstant(fieldName) != null) {
            1
        } else {
            val fieldName2 = predicate.equatesWithField(fieldName)
            if (fieldName2 != null) {
                plan.distinctValues(fieldName).coerceAtMost(plan.distinctValues(fieldName2))
            } else {
                plan.distinctValues(fieldName)
            }
        }
    }

    override fun schema(): Schema {
        return plan.schema()
    }
}
