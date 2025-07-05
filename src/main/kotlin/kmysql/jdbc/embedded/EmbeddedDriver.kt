package kmysql.jdbc.embedded

import kmysql.jdbc.DriverAdapter
import kmysql.server.KMySQL
import java.sql.Connection
import java.sql.SQLException
import java.util.*

class EmbeddedDriver : DriverAdapter() {
    override fun connect(url: String?, info: Properties?): Connection {
        if (url == null) {
            throw SQLException("url is null")
        }
        val db = KMySQL(url)
        return EmbeddedConnection(db)
    }
}
