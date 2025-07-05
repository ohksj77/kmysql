package kmysql.plan

import kmysql.parse.QueryData
import kmysql.transaction.Transaction

interface QueryPlanner {
    fun createPlan(data: QueryData, transaction: Transaction): Plan
}
