package kmysql.parse

import java.io.IOException
import java.io.StreamTokenizer
import java.io.StringReader

class Lexer(string: String) {
    private lateinit var keywords: MutableCollection<String>
    private var tokenizer: StreamTokenizer

    init {
        initKeywords()
        tokenizer = StreamTokenizer(StringReader(string))
        tokenizer.ordinaryChar('.'.code)
        tokenizer.wordChars('_'.code, '_'.code)
        tokenizer.lowerCaseMode(true)
        nextToken()
    }

    fun matchDelimiter(delimiter: Char): Boolean {
        return delimiter == (tokenizer.ttype.toChar())
    }

    fun matchStar(): Boolean {
        return tokenizer.ttype == '*'.code
    }

    fun matchIntConstant(): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_NUMBER
    }

    fun matchStringConstant(): Boolean {
        return '\'' == tokenizer.ttype.toChar()
    }

    fun matchKeyword(word: String): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && 
               tokenizer.sval.equals(word, ignoreCase = true)
    }

    fun matchId(): Boolean {
        return tokenizer.ttype == StreamTokenizer.TT_WORD && !keywords.contains(tokenizer.sval)
    }

    fun eatDelimiter(delimiter: Char) {
        if (!matchDelimiter(delimiter)) {
            throw BadSyntaxException()
        }
        nextToken()
    }

    fun eatStar() {
        if (!matchStar()) {
            throw BadSyntaxException()
        }
        nextToken()
    }

    fun eatIntConstant(): Int {
        if (!matchIntConstant()) {
            throw BadSyntaxException()
        }
        val i = tokenizer.nval.toInt()
        nextToken()
        return i
    }

    fun eatStringConstant(): String {
        if (!matchStringConstant()) {
            throw BadSyntaxException()
        }
        val s = tokenizer.sval
        nextToken()
        return s
    }

    fun eatKeyword(word: String) {
        if (!matchKeyword(word)) {
            throw BadSyntaxException()
        }
        nextToken()
    }

    fun eatId(): String {
        if (!matchId()) {
            throw BadSyntaxException()
        }
        val s = tokenizer.sval
        nextToken()
        return s
    }

    private fun nextToken() {
        try {
            tokenizer.nextToken()
        } catch (e: IOException) {
            throw BadSyntaxException()
        }
    }

    private fun initKeywords() {
        keywords = mutableListOf(
            "select", "from", "where", "and", "insert", "into", "values", "delete",
            "update", "set", "create", "table", "varchar", "int", "view", "as", "index", "on"
        )
    }
}
