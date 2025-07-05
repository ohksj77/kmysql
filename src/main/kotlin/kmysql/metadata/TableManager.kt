package kmysql.metadata

import kmysql.file.BlockId
import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.record.TableScan
import kmysql.transaction.Transaction
import kmysql.util.ConsoleLogger

class TableManager(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    private var tableCatalogLayout: Layout
    private var fieldCatalogLayout: Layout

    init {
        val tableCatalogSchema = tableCatalogSchema()
        tableCatalogLayout = Layout(tableCatalogSchema)

        val fieldCatalogSchema = fieldCatalogSchema()
        fieldCatalogLayout = Layout(fieldCatalogSchema)

        if (isNew) {
            createTable(TABLE_CATALOG_NAME, tableCatalogSchema, transaction)
            createTable(FIELD_CATALOG_NAME, fieldCatalogSchema, transaction)
            createTable(INDEX_CATALOG_NAME, Schema().apply {
                addStringField("indexname", MAX_NAME)
                addStringField("tablename", MAX_NAME)
                addStringField("fieldname", MAX_NAME)
            }, transaction)
        }
    }

    private fun tableCatalogSchema(): Schema {
        val schema = Schema()
        schema.addStringField(TABLE_NAME_FIELD, MAX_NAME)
        schema.addIntField(SLOT_SIZE_FIELD)
        return schema
    }

    private fun fieldCatalogSchema(): Schema {
        val schema = Schema()
        schema.addStringField(TABLE_NAME_FIELD, MAX_NAME)
        schema.addStringField(FIELD_NAME_FIELD, MAX_NAME)
        schema.addIntField(TYPE_FIELD)
        schema.addIntField(LENGTH_FIELD)
        schema.addIntField(OFFSET_FIELD)
        return schema
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction): Unit {
        val layout = Layout(schema)
        val isSystemTable =
            tableName in listOf(TABLE_CATALOG_NAME, FIELD_CATALOG_NAME, INDEX_CATALOG_NAME, "viewcatalog")

        val fileName = if (isSystemTable) tableName else "$tableName.tbl"
        val blockId = BlockId(fileName, 0)
        tx.pin(blockId)
        tx.unpin(blockId)

        val tcat = TableScan(tx, TABLE_CATALOG_NAME, tableCatalogLayout)
        tcat.insert()
        tcat.setString(TABLE_NAME_FIELD, tableName)
        tcat.setInt(SLOT_SIZE_FIELD, layout.slotSize())
        tcat.close()

        val fcat = TableScan(tx, FIELD_CATALOG_NAME, fieldCatalogLayout)
        for (fieldName in schema.fields) {
            var exists = false
            val checkScan = TableScan(tx, FIELD_CATALOG_NAME, fieldCatalogLayout)
            while (checkScan.next()) {
                if (checkScan.getString(TABLE_NAME_FIELD) == tableName &&
                    checkScan.getString(FIELD_NAME_FIELD) == fieldName
                ) {
                    exists = true
                    break
                }
            }
            checkScan.close()
            if (exists) continue

            fcat.insert()
            fcat.setString(TABLE_NAME_FIELD, tableName)
            fcat.setString(FIELD_NAME_FIELD, fieldName)
            val fieldType = schema.type(fieldName) ?: java.sql.Types.VARCHAR
            fcat.setInt(TYPE_FIELD, fieldType)
            fcat.setInt(LENGTH_FIELD, schema.length(fieldName) ?: 0)
            fcat.setInt(OFFSET_FIELD, layout.offset(fieldName) ?: 0)
        }
        fcat.close()

        tx.fm.sync()
        tx.bm.flushAll(tx.getTransactionId())
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        for (fieldName in tableCatalogLayout.schema().fields) {
            val offset = tableCatalogLayout.offset(fieldName)
            val type = tableCatalogLayout.schema().type(fieldName)
            val length = tableCatalogLayout.schema().length(fieldName)
        }

        val tcat = TableScan(tx, TABLE_CATALOG_NAME, tableCatalogLayout)
        var size = -1
        var recordCount = 0
        while (tcat.next()) {
            recordCount++
            val foundTableName = tcat.getString(TABLE_NAME_FIELD)
            if (foundTableName == tableName) {
                size = tcat.getInt(SLOT_SIZE_FIELD)
                break
            }
        }
        tcat.close()

        if (size == -1) {
            throw IllegalArgumentException("테이블을 찾을 수 없습니다: $tableName")
        }

        val schema = getSchema(tableName, tx)
        val layout = Layout(schema)
        return layout
    }

    fun getSchema(tableName: String, tx: Transaction): Schema {
        val schema = Schema()
        val fieldCatalog = TableScan(tx, FIELD_CATALOG_NAME, fieldCatalogLayout)
        while (fieldCatalog.next()) {
            val catalogTableName = fieldCatalog.getString(TABLE_NAME_FIELD)
            if (catalogTableName == tableName) {
                val fieldName = fieldCatalog.getString(FIELD_NAME_FIELD)
                val fieldType = fieldCatalog.getInt(TYPE_FIELD)
                val fieldLength = fieldCatalog.getInt(LENGTH_FIELD)
                schema.addField(fieldName, fieldType, fieldLength)
            }
        }
        fieldCatalog.close()
        return schema
    }

    fun dropTable(tableName: String, tx: Transaction) {
        ConsoleLogger.info("dropTable 시작: $tableName")
        val isSystemTable =
            tableName in listOf(TABLE_CATALOG_NAME, FIELD_CATALOG_NAME, INDEX_CATALOG_NAME, "viewcatalog")
        val fileName = if (isSystemTable) tableName else "$tableName.tbl"

        val file = java.io.File(tx.fm.dbDirectory, fileName)
        if (file.exists()) {
            val deleted = file.delete()
            ConsoleLogger.info("테이블 파일 삭제: $fileName -> $deleted")
        }

        val tcat = TableScan(tx, TABLE_CATALOG_NAME, tableCatalogLayout)
        while (tcat.next()) {
            if (tcat.getString(TABLE_NAME_FIELD) == tableName) {
                tcat.delete()
                break
            }
        }
        tcat.close()

        val fcat = TableScan(tx, FIELD_CATALOG_NAME, fieldCatalogLayout)
        while (fcat.next()) {
            if (fcat.getString(TABLE_NAME_FIELD) == tableName) {
                fcat.delete()
            }
        }
        fcat.close()

        ConsoleLogger.info("dropTable 완료: $tableName")
    }

    companion object {
        const val MAX_NAME = 16
        const val TABLE_CATALOG_NAME = "tablecatalog"
        const val FIELD_CATALOG_NAME = "fieldcatalog"
        const val INDEX_CATALOG_NAME = "indexcatalog"
        const val TABLE_NAME_FIELD = "tablename"
        const val SLOT_SIZE_FIELD = "slotsize"
        const val FIELD_NAME_FIELD = "fieldname"
        const val TYPE_FIELD = "type"
        const val LENGTH_FIELD = "length"
        const val OFFSET_FIELD = "offset"
    }
}
