package kmysql.index.btree

import kmysql.query.Constant

data class DirEntry(
    val dataVal: Constant,
    val blockNumber: Int,
)
