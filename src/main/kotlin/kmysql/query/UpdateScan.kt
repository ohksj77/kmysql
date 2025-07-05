package kmysql.query

import kmysql.record.Rid

interface UpdateScan : Scan {

    fun setVal(fieldName: String, value: Constant)

    fun setInt(fieldName: String, value: Int)

    fun setString(fieldName: String, value: String)

    fun insert()

    fun delete()

    fun getRid(): Rid

    fun moveToRid(rid: Rid)
}
