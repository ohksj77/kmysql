package kmysql.plan

import kmysql.metadata.MetadataManager
import kmysql.metadata.StatisticsInformation
import kmysql.query.Scan
import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.record.TableScan
import kmysql.transaction.Transaction

class TablePlan(
    private val transaction: Transaction,
    private val tableName: String,
    private val metadataManager: MetadataManager,
) : Plan {
    private var layout: Layout = metadataManager.getLayout(tableName, transaction)
    private var statisticsInformation: StatisticsInformation =
        metadataManager.getStatisticsInformation(tableName, layout, transaction)

    override fun open(): Scan {
        return TableScan(transaction, tableName, layout)
    }

    override fun blocksAccessed(): Int {
        return statisticsInformation.blockAccessed()
    }

    override fun recordsOutput(): Int {
        return statisticsInformation.recordsOutput()
    }

    override fun distinctValues(fieldName: String): Int {
        return statisticsInformation.distinctValues(fieldName)
    }

    override fun schema(): Schema {
        return layout.schema()
    }
}
