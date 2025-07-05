package kmysql.transaction.concurrency

import kmysql.file.BlockId

data class Version(
    val blockId: BlockId,
    val data: ByteArray,
    val createdBy: Long,
    val createdAt: Long,
    val deletedBy: Long? = null,
    val deletedAt: Long? = null
) {
    fun isVisible(
        currentTxId: Long,
        snapshotTimestamp: Long,
        isolationLevel: IsolationLevel?,
        committedTransactions: Set<Long>,
        commitTimestamps: Map<Long, Long>,
        rolledBackTransactions: Set<Long>
    ): Boolean {
        if (createdBy == currentTxId) return true

        if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
            if (rolledBackTransactions.contains(createdBy)) {
                return false
            }
            return true
        } else {
            if (!committedTransactions.contains(createdBy)) return false
        }

        if (deletedBy != null) {
            if (deletedBy == currentTxId) return false
            if (!committedTransactions.contains(deletedBy)) return true
            val delCommitTime = commitTimestamps[deletedBy] ?: Long.MAX_VALUE
            if (delCommitTime <= snapshotTimestamp) return false
        }

        val verCommitTime = commitTimestamps[createdBy] ?: Long.MAX_VALUE
        return verCommitTime <= snapshotTimestamp
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) {
            return true
        }
        if (javaClass != other?.javaClass) {
            return false
        }

        other as Version

        if (blockId != other.blockId) {
            return false
        }
        if (!data.contentEquals(other.data)) {
            return false
        }
        if (createdBy != other.createdBy) {
            return false
        }
        if (createdAt != other.createdAt) {
            return false
        }
        if (deletedBy != other.deletedBy) {
            return false
        }
        if (deletedAt != other.deletedAt) {
            return false
        }
        return true
    }

    override fun hashCode(): Int {
        var result = blockId.hashCode()
        result = 31 * result + data.contentHashCode()
        result = 31 * result + createdBy.hashCode()
        result = 31 * result + createdAt.hashCode()
        result = 31 * result + (deletedBy?.hashCode() ?: 0)
        result = 31 * result + (deletedAt?.hashCode() ?: 0)
        return result
    }
}
