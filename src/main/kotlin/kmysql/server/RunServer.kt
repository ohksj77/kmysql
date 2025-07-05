package kmysql.server

import kmysql.jdbc.embedded.EmbeddedDriver
import kmysql.util.ConsoleLogger
import java.io.File
import java.sql.SQLException
import java.sql.Statement
import java.util.*

fun main(args: Array<String>) {
    val scanner = Scanner(System.`in`)
    print("Connect> ")
    val connectionString = scanner.nextLine()

    val dbPath = normalizeDatabasePath(connectionString)
    ConsoleLogger.info("데이터베이스 경로: $dbPath")

    val driver = EmbeddedDriver()
    try {
        val connection = driver.connect(dbPath, null)
        val statement = connection.createStatement()
        print("\nSQL> ")
        while (scanner.hasNextLine()) {
            val cmd = scanner.nextLine().trim()
            if (cmd.equals("exit", ignoreCase = true)) {
                break
            } else if (cmd.equals("commit", ignoreCase = true)) {
                statement.connection.commit()
            } else if (cmd.equals("rollback", ignoreCase = true)) {
                statement.connection.rollback()
            } else if (cmd.startsWith("select", ignoreCase = true)) {
                doQuery(statement, cmd)
            } else {
                doUpdate(statement, cmd)
                if (cmd.trim().startsWith("CREATE", ignoreCase = true) ||
                    cmd.trim().startsWith("DROP", ignoreCase = true) ||
                    cmd.trim().startsWith("ALTER", ignoreCase = true)
                ) {
                    statement.connection.commit()
                    println("OK")
                } else if (cmd.trim().startsWith("INSERT", ignoreCase = true) ||
                    cmd.trim().startsWith("UPDATE", ignoreCase = true) ||
                    cmd.trim().startsWith("DELETE", ignoreCase = true)
                ) {
                    statement.connection.commit()
                    println("OK")
                }
            }
            print("\nSQL> ")
        }
    } catch (e: SQLException) {
        e.printStackTrace()
    }
    scanner.close()
}

private fun normalizeDatabasePath(inputPath: String): String {
    val currentDir = System.getProperty("user.dir")
    ConsoleLogger.info("현재 작업 디렉토리: $currentDir")

    val path = inputPath.trim()
    if (path.isEmpty()) {
        throw IllegalArgumentException("데이터베이스 경로가 비어있습니다.")
    }

    val dbFile = if (path.startsWith("/") || path.startsWith("~")) {
        File(path)
    } else {
        File(currentDir, path)
    }

    val absolutePath = dbFile.absolutePath
    ConsoleLogger.info("정규화된 데이터베이스 경로: $absolutePath")

    return absolutePath
}

private fun doQuery(statement: Statement, cmd: String) {
    try {
        val resultSet = statement.executeQuery(cmd)
        val metaData = resultSet.metaData
        val columnCount = metaData.columnCount

        if (columnCount == 0) {
            println("Empty result set")
            return
        }

        // 컬럼 헤더 출력
        val columnNames = mutableListOf<String>()
        val columnWidths = mutableListOf<Int>()

        for (i in 1..columnCount) {
            val columnName = metaData.getColumnName(i)
            columnNames.add(columnName)
            columnWidths.add(columnName.length)
        }

        // 데이터를 읽어서 컬럼 너비 계산
        val rows = mutableListOf<List<String>>()
        while (resultSet.next()) {
            val row = mutableListOf<String>()
            for (i in 1..columnCount) {
                val fieldType = metaData.getColumnType(i)
                val value = when (fieldType) {
                    java.sql.Types.INTEGER -> resultSet.getInt(i).toString()
                    java.sql.Types.VARCHAR -> resultSet.getString(i) ?: "NULL"
                    else -> resultSet.getString(i) ?: "NULL"
                }
                row.add(value)
                // 컬럼 너비 업데이트
                if (value.length > columnWidths[i - 1]) {
                    columnWidths[i - 1] = value.length
                }
            }
            rows.add(row)
        }

        // 테이블 출력
        if (rows.isEmpty()) {
            println("Empty set")
            return
        }

        // 구분선 출력
        print("+")
        for (width in columnWidths) {
            print("-".repeat(width + 2) + "+")
        }
        println()

        // 헤더 출력
        print("|")
        for (i in columnNames.indices) {
            print(" ${columnNames[i].padEnd(columnWidths[i])} |")
        }
        println()

        // 구분선 출력
        print("+")
        for (width in columnWidths) {
            print("-".repeat(width + 2) + "+")
        }
        println()

        // 데이터 출력
        for (row in rows) {
            print("|")
            for (i in row.indices) {
                print(" ${row[i].padEnd(columnWidths[i])} |")
            }
            println()
        }

        // 구분선 출력
        print("+")
        for (width in columnWidths) {
            print("-".repeat(width + 2) + "+")
        }
        println()

        println("${rows.size} row(s) in set")

    } catch (e: SQLException) {
        ConsoleLogger.error("SQL 오류: ${e.message}")
        e.printStackTrace()
    } catch (e: Exception) {
        ConsoleLogger.error("오류: ${e.message}")
        e.printStackTrace()
    }
}

private fun doUpdate(statement: Statement, cmd: String) {
    try {
        val howMany = statement.executeUpdate(cmd)
    } catch (e: SQLException) {
        ConsoleLogger.error("SQL Exception: ${e.message}")
        e.printStackTrace()
    }
}
