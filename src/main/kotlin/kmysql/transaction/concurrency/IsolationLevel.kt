package kmysql.transaction.concurrency

enum class IsolationLevel(
    val description: String,
) {
    READ_UNCOMMITTED("READ UNCOMMITTED"),
    READ_COMMITTED("READ COMMITTED"),
    REPEATABLE_READ("REPEATABLE READ"),
    SERIALIZABLE("SERIALIZABLE");

    companion object {
        fun fromString(level: String): IsolationLevel {
            return when (level.uppercase()) {
                "READ UNCOMMITTED" -> READ_UNCOMMITTED
                "READ COMMITTED" -> READ_COMMITTED
                "REPEATABLE READ" -> REPEATABLE_READ
                "SERIALIZABLE" -> SERIALIZABLE
                else -> REPEATABLE_READ
            }
        }
    }
}
