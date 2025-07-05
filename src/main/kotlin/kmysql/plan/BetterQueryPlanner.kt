package kmysql.plan

import kmysql.metadata.MetadataManager
import kmysql.parse.Parser
import kmysql.parse.QueryData
import kmysql.transaction.Transaction


class BetterQueryPlanner(
    private val metadataManager: MetadataManager,
) : QueryPlanner {

    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        val plans = mutableListOf<Plan>()
        for (tableName in data.tables) {
            val viewDef = metadataManager.getViewDef(tableName, transaction)
            if (viewDef != null) {
                val parser = Parser(viewDef)
                val viewData = parser.query()
                plans.add(createPlan(viewData, transaction))
            } else {
                plans.add(TablePlan(transaction, tableName, metadataManager))
            }
        }

        var plan = plans.removeAt(0)
        for (nextPlan in plans) {
            val choice1: Plan = ProductPlan(nextPlan, plan)
            val choice2: Plan = ProductPlan(plan, nextPlan)
            plan = if (choice1.blocksAccessed() < choice2.blocksAccessed()) {
                choice1
            } else {
                choice2
            }
        }

        plan = SelectPlan(plan, data.predicate)

        plan = ProjectPlan(plan, data.fields)
        return plan
    }
}
