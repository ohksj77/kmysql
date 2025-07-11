package kmysql.index.planner

import kmysql.metadata.MetadataManager
import kmysql.parse.CreateIndexData
import kmysql.parse.CreateTableData
import kmysql.parse.CreateViewData
import kmysql.parse.DeleteData
import kmysql.parse.InsertData
import kmysql.parse.ModifyData
import kmysql.plan.Plan
import kmysql.plan.SelectPlan
import kmysql.plan.TablePlan
import kmysql.plan.UpdatePlanner
import kmysql.query.UpdateScan
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

class IndexUpdatePlanner(
    private val metadataManager: MetadataManager,
) : UpdatePlanner {

    override fun executeInsert(data: InsertData, transaction: Transaction): Int {
        val tableName = data.tableName
        val plan = TablePlan(transaction, tableName, metadataManager)

        val updateScan = plan.open() as UpdateScan
        updateScan.insert()
        val rid = updateScan.getRid()

        val indexes = metadataManager.getIndexInformation(tableName, transaction)
        val valueIterator = data.values.iterator()
        for (fieldName in data.fields) {
            val value = valueIterator.next()
            ConsoleLogger.info("Modify field $fieldName to val $value")
            updateScan.setVal(fieldName, value)

            val indexInfo = indexes[fieldName]
            if (indexInfo != null) {
                val index = indexInfo.open()
                index.insert(value, rid)
                index.close()
            }
        }
        updateScan.close()
        return 1
    }

    override fun executeDelete(data: DeleteData, transaction: Transaction): Int {
        val tableName = data.tableName
        var plan: Plan = TablePlan(transaction, tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val indexes = metadataManager.getIndexInformation(tableName, transaction)
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            val rid = updateScan.getRid()
            for (fieldName in indexes.keys) {
                val value = updateScan.getVal(fieldName)
                val index = indexes[fieldName]?.open() ?: throw RuntimeException("null error")
                index.delete(value, rid)
                index.close()
            }
            updateScan.delete()
            count += 1
        }
        updateScan.close()
        return count
    }

    override fun executeModify(data: ModifyData, transaction: Transaction): Int {
        val tableName = data.tableName
        val fieldName = data.fieldName
        var plan: Plan = TablePlan(transaction, tableName, metadataManager)
        plan = SelectPlan(plan, data.predicate)
        val indexInfo = metadataManager.getIndexInformation(tableName, transaction)[fieldName]
        val index = indexInfo?.open()
        val updateScan = plan.open() as UpdateScan
        var count = 0
        while (updateScan.next()) {
            val newValue = data.newValue.evaluate(updateScan)
            val oldValue = updateScan.getVal(fieldName)
            updateScan.setVal(data.fieldName, newValue)

            if (index != null) {
                val rid = updateScan.getRid()
                index.delete(oldValue, rid)
                index.insert(newValue, rid)
            }
            count += 1
        }
        index?.close()
        updateScan.close()
        return count
    }

    override fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int {
        metadataManager.createTable(data.tableName, data.schema, transaction)
        return 0
    }

    override fun executeCreateView(data: CreateViewData, transaction: Transaction): Int {
        metadataManager.createView(data.viewName, data.viewDef(), transaction)
        return 0
    }

    override fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int {
        metadataManager.createIndex(data.indexName, data.tableName, data.fieldName, transaction)
        return 0
    }
}
