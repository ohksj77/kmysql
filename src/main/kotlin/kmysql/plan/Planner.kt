package kmysql.plan

import kmysql.parse.CreateIndexData
import kmysql.parse.CreateTableData
import kmysql.parse.CreateViewData
import kmysql.parse.DeleteData
import kmysql.parse.InsertData
import kmysql.parse.ModifyData
import kmysql.parse.Parser
import kmysql.transaction.Transaction


class Planner(
    private val queryPlanner: QueryPlanner,
    private val updatePlanner: UpdatePlanner,
) {
    fun createQueryPlan(cmd: String, transaction: Transaction): Plan {
        try {
            val parser = Parser(cmd)
            val queryData = parser.query()
            return queryPlanner.createPlan(queryData, transaction)
        } catch (e: Exception) {
            e.printStackTrace()
            throw e
        }
    }

    fun executeUpdate(cmd: String, transaction: Transaction): Int {
        val parser = Parser(cmd)

        return when (val updateData = parser.updateCmd()) {
            is InsertData -> {
                updatePlanner.executeInsert(updateData, transaction)
            }

            is DeleteData -> {
                updatePlanner.executeDelete(updateData, transaction)
            }

            is ModifyData -> {
                updatePlanner.executeModify(updateData, transaction)
            }

            is CreateTableData -> {
                updatePlanner.executeCreateTable(updateData, transaction)
            }

            is CreateViewData -> {
                updatePlanner.executeCreateView(updateData, transaction)
            }

            is CreateIndexData -> {
                updatePlanner.executeCreateIndex(updateData, transaction)
            }

            else -> {
                0
            }
        }
    }
}
