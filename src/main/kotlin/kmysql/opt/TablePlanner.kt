package kmysql.opt

import kmysql.index.planner.IndexJoinPlan
import kmysql.index.planner.IndexSelectPlan
import kmysql.metadata.MetadataManager
import kmysql.multibuffer.MultiBufferProductPlan
import kmysql.plan.Plan
import kmysql.plan.SelectPlan
import kmysql.plan.TablePlan
import kmysql.query.Predicate
import kmysql.record.Schema
import kmysql.transaction.Transaction

class TablePlanner(
    private val tableName: String,
    private val myPredicate: Predicate,
    private val transaction: Transaction,
    private val metadataManager: MetadataManager,
) {
    private val myTablePlan = TablePlan(transaction, tableName, metadataManager)
    private val mySchema = myTablePlan.schema()
    private val indexes = metadataManager.getIndexInformation(tableName, transaction)

    fun makeSelectPlan(): Plan {
        var plan = makeIndexSelect()
        if (plan == null) plan = myTablePlan
        return addSelectPredicate(plan)
    }

    fun makeJoinPlan(current: Plan): Plan? {
        val currentSchema = current.schema()
        myPredicate.joinSubPredicate(mySchema, currentSchema) ?: return null
        var plan = makeIndexJoin(current, currentSchema)
        if (plan == null) {
            plan = makeProductJoin(current, currentSchema)
        }
        return plan
    }

    fun makeProductPlan(current: Plan): Plan {
        val plan = addSelectPredicate(myTablePlan)
        return MultiBufferProductPlan(transaction, current, plan)
    }

    private fun makeIndexSelect(): Plan? {
        for (fieldName in indexes.keys) {
            val value = myPredicate.equateWithConstant(fieldName)
            if (value != null) {
                val indexInfo = indexes[fieldName] ?: throw RuntimeException("null error")
                return IndexSelectPlan(myTablePlan, indexInfo, value)
            }
        }
        return null
    }

    private fun makeIndexJoin(current: Plan, currentSchema: Schema): Plan? {
        for (fieldName in indexes.keys) {
            val outerField = myPredicate.equatesWithField(fieldName)
            if (outerField != null && currentSchema.hasField(outerField)) {
                val indexInfo = indexes[fieldName] ?: throw RuntimeException("null error")
                var plan: Plan = IndexJoinPlan(current, myTablePlan, outerField, indexInfo)
                plan = addSelectPredicate(plan)
                return addJoinPredicate(plan, currentSchema)
            }
        }
        return null
    }

    private fun makeProductJoin(current: Plan, currentSchema: Schema): Plan {
        val plan = makeProductPlan(current)
        return addJoinPredicate(plan, currentSchema)
    }

    private fun addSelectPredicate(plan: Plan): Plan {
        val selectPredicate = myPredicate.selectSubPredicate(mySchema)
        return if (selectPredicate != null) {
            SelectPlan(plan, selectPredicate)
        } else {
            plan
        }
    }

    private fun addJoinPredicate(plan: Plan, currentSchema: Schema): Plan {
        val joinPredicate = myPredicate.joinSubPredicate(currentSchema, mySchema)
        return if (joinPredicate != null) {
            SelectPlan(plan, joinPredicate)
        } else {
            plan
        }
    }
}
