package kmysql.plan

import kmysql.query.Scan
import kmysql.record.Schema

interface Plan {

    fun open(): Scan

    fun blocksAccessed(): Int

    fun recordsOutput(): Int

    fun distinctValues(fieldName: String): Int

    fun schema(): Schema
}
