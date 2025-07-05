package kmysql.parse

import kmysql.query.Constant
import kmysql.query.Expression
import kmysql.query.Predicate
import kmysql.query.Term
import kmysql.record.Schema

class Parser(private val string: String) {
    private val lexer = Lexer(string)

    fun field(): String {
        return lexer.eatId()
    }

    fun constant(): Constant {
        return if (lexer.matchStringConstant()) {
            Constant(lexer.eatStringConstant())
        } else {
            Constant(lexer.eatIntConstant())
        }
    }

    fun expression(): Expression {
        return if (lexer.matchId()) {
            Expression(field())
        } else {
            Expression(constant())
        }
    }

    fun term(): Term {
        val leftSideExpression = expression()
        lexer.eatDelimiter('=')
        val rightSideExpression = expression()
        return Term(leftSideExpression, rightSideExpression)
    }

    fun predicate(): Predicate {
        val predicate = Predicate(term())
        if (lexer.matchKeyword("and")) {
            lexer.eatKeyword("and")
            predicate.conjoinWith(predicate())
        }
        return predicate
    }

    fun query(): QueryData {
        lexer.eatKeyword("select")
        val fields = selectList()
        lexer.eatKeyword("from")
        val tables = tableList()
        var predicate = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            predicate = predicate()
        }
        val queryData = QueryData(fields, tables, predicate)
        return queryData
    }

    private fun selectList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        if (lexer.matchStar()) {
            lexer.eatStar()
            mutableList.add("*")
        } else {
            mutableList.add(field())
            if (lexer.matchDelimiter(',')) {
                lexer.eatDelimiter(',')
                mutableList.addAll(selectList())
            }
        }
        return mutableList
    }

    private fun tableList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        val tableName = lexer.eatId()
        mutableList.add(tableName)
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(tableList())
        }
        return mutableList
    }

    fun updateCmd(): Any {
        return if (lexer.matchKeyword("insert")) {
            insert()
        } else if (lexer.matchKeyword("delete")) {
            delete()
        } else if (lexer.matchKeyword("update")) {
            modify()
        } else {
            create()
        }
    }

    private fun create(): Any {
        lexer.eatKeyword("create")
        return if (lexer.matchKeyword("table")) {
            createTable()
        } else if (lexer.matchKeyword("view")) {
            createView()
        } else {
            createIndex()
        }
    }

    fun delete(): DeleteData {
        lexer.eatKeyword("delete")
        lexer.eatKeyword("from")
        val tableName = lexer.eatId()
        var predicate = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            predicate = predicate()
        }
        return DeleteData(tableName, predicate)
    }

    fun insert(): InsertData {
        lexer.eatKeyword("insert")
        lexer.eatKeyword("into")
        val tableName = lexer.eatId()

        val fields = if (lexer.matchDelimiter('(')) {
            lexer.eatDelimiter('(')
            val fieldList = fieldList()
            lexer.eatDelimiter(')')
            fieldList
        } else {
            mutableListOf()
        }

        lexer.eatKeyword("values")
        lexer.eatDelimiter('(')
        val values = constList()
        lexer.eatDelimiter(')')
        return InsertData(tableName, fields, values)
    }

    private fun fieldList(): MutableList<String> {
        val mutableList = mutableListOf<String>()
        mutableList.add(field())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(fieldList())
        }
        return mutableList
    }

    private fun constList(): MutableList<Constant> {
        val mutableList = mutableListOf<Constant>()
        mutableList.add(constant())
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            mutableList.addAll(constList())
        }
        return mutableList
    }

    fun modify(): ModifyData {
        lexer.eatKeyword("update")
        val tableName = lexer.eatId()
        lexer.eatKeyword("set")
        val fieldName = field()
        lexer.eatDelimiter('=')
        val newValue = expression()
        var predicate = Predicate()
        if (lexer.matchKeyword("where")) {
            lexer.eatKeyword("where")
            predicate = predicate()
        }
        return ModifyData(tableName, fieldName, newValue, predicate)
    }

    fun createTable(): CreateTableData {
        lexer.eatKeyword("table")
        val tableName = lexer.eatId()
        lexer.eatDelimiter('(')
        val schema = fieldDefs()
        lexer.eatDelimiter(')')
        return CreateTableData(tableName, schema)
    }

    private fun fieldDefs(): Schema {
        val schema = fieldDef()
        if (lexer.matchDelimiter(',')) {
            lexer.eatDelimiter(',')
            val schema2 = fieldDefs()
            schema.addAll(schema2)
        }
        return schema
    }

    private fun fieldDef(): Schema {
        val fieldName = field()
        return fieldType(fieldName)
    }

    private fun fieldType(fieldName: String): Schema {
        val schema = Schema()
        if (lexer.matchKeyword("int")) {
            lexer.eatKeyword("int")
            schema.addIntField(fieldName)
        } else {
            lexer.eatKeyword("varchar")
            lexer.eatDelimiter('(')
            val stringLength = lexer.eatIntConstant()
            lexer.eatDelimiter(')')
            schema.addStringField(fieldName, stringLength)
        }
        return schema
    }

    fun createView(): CreateViewData {
        lexer.eatKeyword("view")
        val viewName = lexer.eatId()
        lexer.eatKeyword("as")
        val queryData = query()
        return CreateViewData(viewName, queryData)
    }

    fun createIndex(): CreateIndexData {
        lexer.eatKeyword("index")
        val indexName = lexer.eatId()
        lexer.eatKeyword("on")
        val tableName = lexer.eatId()
        lexer.eatDelimiter('(')
        val fieldName = field()
        lexer.eatDelimiter(')')
        return CreateIndexData(indexName, tableName, fieldName)
    }
}
