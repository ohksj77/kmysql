package kmysql.record

import kmysql.file.Page
import java.util.concurrent.ConcurrentHashMap

class Layout {
    private var schema: Schema
    private var offsets = ConcurrentHashMap<String, Int>()
    private var slotSize: Int = 0

    constructor(schema: Schema) {
        this.schema = schema
        var pos = Integer.BYTES
        for (fieldName in schema.fields) {
            offsets[fieldName] = pos
            pos += lengthInBytes(fieldName)
        }
        this.slotSize = pos
    }

    constructor(schema: Schema, offsets: ConcurrentHashMap<String, Int>, slotSize: Int) {
        this.schema = schema
        this.offsets = offsets
        this.slotSize = slotSize
    }

    fun offset(fieldName: String): Int? {
        return offsets[fieldName]
    }

    fun slotSize(): Int {
        return slotSize
    }

    fun schema(): Schema {
        return schema
    }

    private fun lengthInBytes(fieldName: String): Int {
        val fieldType = schema.type(fieldName)
        if (fieldType == java.sql.Types.INTEGER) return Integer.BYTES
        val schemaLength = schema.length(fieldName) ?: throw IllegalArgumentException("Cannot find specified field")
        return Integer.BYTES + schemaLength
    }
}
