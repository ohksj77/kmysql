package kmysql.index

import kmysql.query.Constant
import kmysql.record.Rid

interface Index {

    fun beforeFirst(searchKey: Constant)

    fun next(): Boolean

    fun getDataRid(): Rid

    fun insert(dataValue: Constant, dataRid: Rid)

    fun delete(dataValue: Constant, dataRid: Rid)

    fun close()
}
