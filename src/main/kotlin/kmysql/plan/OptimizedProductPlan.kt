package kmysql.plan

import kmysql.query.Scan
import kmysql.record.Schema

class OptimizedProductPlan(
    private val plan1: Plan,
    private val plan2: Plan,
) : Plan {
    private var bestPlan: Plan

    init {
        val productPlan1 = ProductPlan(plan1, plan2)
        val productPlan2 = ProductPlan(plan2, plan1)
        val blockAccessed1 = productPlan1.blocksAccessed()
        val blockAccessed2 = productPlan2.blocksAccessed()
        bestPlan = if (blockAccessed1 < blockAccessed2) {
            productPlan1
        } else {
            productPlan2
        }
    }

    override fun open(): Scan = bestPlan.open()

    override fun blocksAccessed(): Int = bestPlan.blocksAccessed()

    override fun recordsOutput(): Int = bestPlan.recordsOutput()

    override fun distinctValues(fieldName: String): Int = bestPlan.distinctValues(fieldName)

    override fun schema(): Schema = bestPlan.schema()
}
