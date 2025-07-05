package kmysql.opt

import kmysql.metadata.MetadataManager
import kmysql.parse.QueryData
import kmysql.plan.Plan
import kmysql.plan.ProjectPlan
import kmysql.plan.QueryPlanner
import kmysql.transaction.Transaction

class HeuristicQueryPlanner(
    private val metadataManager: MetadataManager,
) : QueryPlanner {
    private val tablePlanners = mutableListOf<TablePlanner>()

    override fun createPlan(data: QueryData, transaction: Transaction): Plan {
        for (tableName in data.tables) {
            val tablePlanner = TablePlanner(tableName, data.predicate, transaction, metadataManager)
            tablePlanners.add(tablePlanner)
        }

        var currentPlan = getLowestSelectPlan()

        while (tablePlanners.isNotEmpty()) {
            val plan = getLowestJoinPlan(currentPlan)
            currentPlan = plan
        }

        return ProjectPlan(currentPlan, data.fields)
    }

    private fun getLowestSelectPlan(): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tablePlanner in tablePlanners) {
            val plan = tablePlanner.makeSelectPlan()
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTablePlanner = tablePlanner
                bestPlan = plan
            }
        }
        tablePlanners.remove(bestTablePlanner)
        return bestPlan ?: throw RuntimeException("null error")
    }

    private fun getLowestJoinPlan(current: Plan): Plan {
        val bestTablePlanner: TablePlanner? = null
        val bestPlan: Plan? = null
        for (tablePlanner in tablePlanners) {
            val plan = tablePlanner.makeJoinPlan(current) ?: throw RuntimeException("null error")
        }
        return bestPlan ?: throw RuntimeException("null error")
    }

    private fun getLowestProductPlan(current: Plan): Plan {
        var bestTablePlanner: TablePlanner? = null
        var bestPlan: Plan? = null
        for (tablePlanner in tablePlanners) {
            val plan = tablePlanner.makeProductPlan(current)
            if (bestPlan == null || plan.recordsOutput() < bestPlan.recordsOutput()) {
                bestTablePlanner = tablePlanner
                bestPlan = plan
            }
        }
        tablePlanners.remove(bestTablePlanner)
        return bestPlan ?: throw RuntimeException("null error")
    }
}
