package kmysql.util

import java.time.LocalDateTime

object ConsoleLogger {

    enum class Level { DEBUG, INFO, WARN, ERROR }

    private val level: Level = Level.INFO

    fun debug(message: String) = log(Level.DEBUG, message)
    fun info(msg: String) = log(Level.INFO, msg)
    fun warn(msg: String) = log(Level.WARN, msg)
    fun error(msg: String) = log(Level.ERROR, msg)

    private fun log(messageLevel: Level, message: String) {
        if (messageLevel.ordinal >= level.ordinal) {
            val timestamp = LocalDateTime.now()
            println("[$timestamp][${messageLevel.name}] $message")
        }
    }
}
