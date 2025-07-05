package kmysql.plan

import kmysql.parse.CreateIndexData
import kmysql.parse.CreateTableData
import kmysql.parse.CreateViewData
import kmysql.parse.DeleteData
import kmysql.parse.InsertData
import kmysql.parse.ModifyData
import kmysql.transaction.Transaction

interface UpdatePlanner {
    fun executeInsert(data: InsertData, transaction: Transaction): Int

    fun executeDelete(data: DeleteData, transaction: Transaction): Int

    fun executeModify(data: ModifyData, transaction: Transaction): Int

    fun executeCreateTable(data: CreateTableData, transaction: Transaction): Int

    fun executeCreateView(data: CreateViewData, transaction: Transaction): Int

    fun executeCreateIndex(data: CreateIndexData, transaction: Transaction): Int
}
