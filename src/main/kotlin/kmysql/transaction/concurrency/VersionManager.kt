package kmysql.transaction.concurrency

import kmysql.file.BlockId
import kmysql.util.ConsoleLogger
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

class VersionManager {
    private val versions = ConcurrentHashMap<BlockId, MutableList<Version>>()
    private val globalTimestamp = AtomicLong(0L)
    private val committedTransactions = ConcurrentHashMap.newKeySet<Long>()
    private val commitTimestamps = ConcurrentHashMap<Long, Long>()
    private val rolledBackTransactions = ConcurrentHashMap.newKeySet<Long>()

    companion object {
        @Volatile
        private var instance: VersionManager? = null

        fun getInstance(): VersionManager {
            return instance ?: synchronized(this) {
                instance ?: VersionManager().also { instance = it }
            }
        }

        fun reset() {
            synchronized(this) {
                instance = null
            }
        }
    }

    fun getNextTimestamp(): Long {
        return globalTimestamp.incrementAndGet()
    }

    fun createVersion(blockId: BlockId, data: ByteArray, transactionId: Long): Version {
        val timestamp = getNextTimestamp()
        val version = Version(
            blockId = blockId,
            data = data.copyOf(),
            createdBy = transactionId,
            createdAt = timestamp
        )

        versions.computeIfAbsent(blockId) { mutableListOf() }.add(version)
        ConsoleLogger.info("VersionManager.createVersion: blockId=$blockId, transactionId=$transactionId, timestamp=$timestamp, totalVersions=${versions[blockId]?.size}")
        return version
    }

    fun getVisibleVersion(
        blockId: BlockId,
        transactionId: Long,
        readTimestamp: Long,
        isolationLevel: IsolationLevel?,
        txStartTimestamp: Long?
    ): Version? {
        val blockVersions = versions[blockId] ?: return null
        val snapshotTimestamp = when (isolationLevel) {
            IsolationLevel.READ_COMMITTED -> globalTimestamp.get()
            IsolationLevel.REPEATABLE_READ, IsolationLevel.SERIALIZABLE -> txStartTimestamp ?: readTimestamp
            else -> globalTimestamp.get()
        }
        val visibleVersions = blockVersions.filter { version ->
            version.isVisible(
                transactionId,
                snapshotTimestamp,
                isolationLevel,
                committedTransactions,
                commitTimestamps,
                rolledBackTransactions
            )
        }
        val result = visibleVersions.maxByOrNull { it.createdAt }
        ConsoleLogger.info("getVisibleVersion: blockId=$blockId, transactionId=$transactionId, snapshotTimestamp=$snapshotTimestamp, isolationLevel=$isolationLevel, allVersions=${blockVersions.size}, visibleVersions=${visibleVersions.size}, result=$result")
        return result
    }

    fun markAsDeleted(blockId: BlockId, transactionId: Long): Boolean {
        val blockVersions = versions[blockId] ?: return false
        val timestamp = getNextTimestamp()

        val latestVersion = blockVersions.maxByOrNull { it.createdAt }
        if (latestVersion != null && latestVersion.deletedBy == null) {
            val deletedVersion = latestVersion.copy(
                deletedBy = transactionId,
                deletedAt = timestamp
            )
            blockVersions.add(deletedVersion)
            return true
        }
        return false
    }

    fun cleanupOldVersions(olderThanTimestamp: Long) {
        versions.forEach { (blockId, blockVersions) ->
            blockVersions.removeAll { version ->
                version.createdAt < olderThanTimestamp &&
                        (version.deletedAt == null || version.deletedAt < olderThanTimestamp)
            }
        }
    }

    fun getVersionCount(blockId: BlockId): Int {
        return versions[blockId]?.size ?: 0
    }

    fun getAllVersions(blockId: BlockId): List<Version> {
        return versions[blockId]?.toList() ?: emptyList()
    }

    fun rollbackTo(blockId: BlockId, transactionId: Long, afterTimestamp: Long? = null) {
        val blockVersions = versions[blockId] ?: return
        if (afterTimestamp != null) {
            blockVersions.removeIf {
                it.createdBy == transactionId && it.createdAt > afterTimestamp
            }
        } else {
            blockVersions.removeIf { it.createdBy == transactionId }
        }
    }

    fun rollbackToSavepoint(transactionId: Long, savepointTimestamp: Long) {
        versions.forEach { (blockId, blockVersions) ->
            blockVersions.removeIf {
                it.createdBy == transactionId && it.createdAt > savepointTimestamp
            }
        }
    }

    fun commitTransaction(transactionId: Long) {
        committedTransactions.add(transactionId)
        commitTimestamps[transactionId] = globalTimestamp.incrementAndGet()
    }

    fun rollbackTransaction(transactionId: Long) {
        committedTransactions.remove(transactionId)
        rolledBackTransactions.add(transactionId)
        versions.forEach { (_, blockVersions) ->
            blockVersions.removeIf {
                it.createdBy == transactionId || it.deletedBy == transactionId
            }
        }
    }

    fun isCommitted(transactionId: Long): Boolean = committedTransactions.contains(transactionId)
}
