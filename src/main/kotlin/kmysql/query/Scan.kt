package kmysql.query

import java.io.Closeable

interface Scan : Closeable {

    fun beforeFirst()

    fun next(): Boolean

    fun getInt(fieldName: String): Int

    fun getString(fieldName: String): String

    fun getVal(fieldName: String): Constant

    fun hasField(fieldName: String): Boolean
}
