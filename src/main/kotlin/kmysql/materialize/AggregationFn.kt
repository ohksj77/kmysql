package kmysql.materialize

import kmysql.query.Constant
import kmysql.query.Scan

interface AggregationFn {
    fun processFirst(scan: Scan)

    fun processNext(scan: Scan)

    fun fieldName(): String

    fun value(): Constant
}
