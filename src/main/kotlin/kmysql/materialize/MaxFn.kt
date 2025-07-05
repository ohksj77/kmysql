package kmysql.materialize

import kmysql.query.Constant
import kmysql.query.Scan

class MaxFn(
    private val fieldName: String
) : AggregationFn {
    private lateinit var value: Constant

    override fun processFirst(scan: Scan) {
        value = scan.getVal(fieldName)
    }

    override fun processNext(scan: Scan) {
        val newValue = scan.getVal(fieldName)
        if (newValue > value) {
            value = newValue
        }
    }

    override fun fieldName(): String {
        return "maxof$fieldName"
    }

    override fun value(): Constant {
        return value
    }
}
