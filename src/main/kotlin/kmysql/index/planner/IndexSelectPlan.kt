package kmysql.index.planner

import kmysql.index.query.IndexSelectScan
import kmysql.metadata.IndexInfo
import kmysql.plan.Plan
import kmysql.query.Constant
import kmysql.query.Scan
import kmysql.record.Schema
import kmysql.record.TableScan

class IndexSelectPlan(
    private val plan: Plan,
    private val indexInfo: IndexInfo,
    private val value: Constant,
) : Plan {

    override fun open(): Scan {
        val tableScan = plan.open() as TableScan
        val index = indexInfo.open()
        return IndexSelectScan(tableScan, index, value)
    }

    override fun blocksAccessed(): Int {
        return indexInfo.blocksAccessed() + recordsOutput()
    }

    override fun recordsOutput(): Int {
        return indexInfo.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return indexInfo.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return plan.schema()
    }
}
