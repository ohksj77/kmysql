package kmysql.metadata

import kmysql.record.Layout
import kmysql.record.Schema
import kmysql.record.TableScan
import kmysql.transaction.Transaction

class TableManager(
    private val isNew: Boolean,
    private val transaction: Transaction,
) {
    private var tableCatalogLayout: Layout
    private var fieldCatalogLayout: Layout

    init {
        val tableCatalogSchema = Schema()
        tableCatalogSchema.addStringField(TABLE_NAME_FIELD, MAX_NAME)
        tableCatalogSchema.addIntField(SLOT_SIZE_FIELD)
        tableCatalogLayout = Layout(tableCatalogSchema)

        val fieldCatalogSchema = Schema()
        fieldCatalogSchema.addStringField(TABLE_NAME_FIELD, MAX_NAME)
        fieldCatalogSchema.addStringField(FIELD_NAME_FIELD, MAX_NAME)
        fieldCatalogSchema.addIntField(TYPE_FIELD)
        fieldCatalogSchema.addIntField(LENGTH_FIELD)
        fieldCatalogSchema.addIntField(OFFSET_FIELD)
        fieldCatalogLayout = Layout(fieldCatalogSchema)

        if (isNew) {
            createTable(TABLE_CATALOG_NAME, tableCatalogSchema, transaction)
            createTable(FIELD_CATALOG_NAME, fieldCatalogSchema, transaction)
        }
    }

    fun createTable(tableName: String, schema: Schema, tx: Transaction) {
        val layout = Layout(schema)

        val tableCatalog = TableScan(tx, TABLE_CATALOG_NAME, tableCatalogLayout)
        tableCatalog.insert()
        tableCatalog.setString(TABLE_NAME_FIELD, tableName)
        tableCatalog.setInt(SLOT_SIZE_FIELD, layout.slotSize())
        tableCatalog.close()

        val fieldCatalog = TableScan(tx, FIELD_CATALOG_NAME, fieldCatalogLayout)
        for (fieldName in schema.fields) {
            fieldCatalog.insert()
            fieldCatalog.setString(TABLE_NAME_FIELD, tableName)
            fieldCatalog.setString(FIELD_NAME_FIELD, fieldName)
            val schemaType = schema.type(fieldName) ?: throw IllegalArgumentException("null schema type")
            fieldCatalog.setInt(TYPE_FIELD, schemaType)
            val schemaLength = schema.length(fieldName) ?: throw IllegalArgumentException("null schema type")
            fieldCatalog.setInt(LENGTH_FIELD, schemaLength)
            val layoutOffset = layout.offset(fieldName) ?: throw IllegalArgumentException("null schema type")
            fieldCatalog.setInt(OFFSET_FIELD, layoutOffset)
        }
        fieldCatalog.close()
    }

    fun getLayout(tableName: String, tx: Transaction): Layout {
        var size = -1
        val tableCatalog = TableScan(tx, TABLE_CATALOG_NAME, tableCatalogLayout)
        while (tableCatalog.next()) {
            if (tableCatalog.getString(TABLE_NAME_FIELD) == tableName) {
                size = tableCatalog.getInt(SLOT_SIZE_FIELD)
                break
            }
        }
        tableCatalog.close()
        val schema = Schema()
        val offsets = mutableMapOf<String, Int>()
        val fieldCatalog = TableScan(tx, FIELD_CATALOG_NAME, fieldCatalogLayout)
        while (fieldCatalog.next()) {
            if (fieldCatalog.getString(TABLE_NAME_FIELD) == tableName) {
                val fieldName = fieldCatalog.getString(FIELD_NAME_FIELD)
                val fieldType = fieldCatalog.getInt(TYPE_FIELD)
                val fieldLength = fieldCatalog.getInt(LENGTH_FIELD)
                val offset = fieldCatalog.getInt(OFFSET_FIELD)
                offsets[fieldName] = offset
                schema.addField(fieldName, fieldType, fieldLength)
            }
        }
        fieldCatalog.close()
        return Layout(schema, offsets, size)
    }

    companion object {
        const val MAX_NAME = 16
        const val TABLE_CATALOG_NAME = "tablecatalog"
        const val FIELD_CATALOG_NAME = "fieldcatalog"
        const val TABLE_NAME_FIELD = "tablename"
        const val SLOT_SIZE_FIELD = "slotsize"
        const val FIELD_NAME_FIELD = "fieldname"
        const val TYPE_FIELD = "type"
        const val LENGTH_FIELD = "length"
        const val OFFSET_FIELD = "offset"
    }
}
