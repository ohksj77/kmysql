package kmysql.materialize

import kmysql.query.Constant
import kmysql.query.Scan

class CountFn(
    private val fieldName: String,
) : AggregationFn {
    private var count: Int = 0

    override fun processFirst(scan: Scan) {
        count = 1
    }

    override fun processNext(scan: Scan) {
        count += 1
    }

    override fun fieldName(): String {
        return "countof$fieldName"
    }

    override fun value(): Constant {
        return Constant(count)
    }
}
