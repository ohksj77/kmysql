package kmysql.transaction

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

object TransactionIdGenerator {
    private val sequenceCounter = AtomicInteger(0)
    private val lastTimestamp = AtomicLong(0L)

    fun generateTransactionId(): Long {
        val currentTimestamp = System.currentTimeMillis()
        val sequence = sequenceCounter.incrementAndGet() and 0xFFFF
        return (currentTimestamp shl 16) or (sequence.toLong() and 0xFFFF)
    }

    fun extractTimestamp(transactionId: Long): Long {
        return transactionId shr 16
    }

    fun extractSequence(transactionId: Long): Int {
        return (transactionId and 0xFFFF).toInt()
    }

    fun compareByTimestamp(id1: Long, id2: Long): Int {
        val timestamp1 = extractTimestamp(id1)
        val timestamp2 = extractTimestamp(id2)
        return timestamp1.compareTo(timestamp2)
    }

    fun isValidTransactionId(transactionId: Long): Boolean {
        val timestamp = extractTimestamp(transactionId)
        val sequence = extractSequence(transactionId)

        val currentTime = System.currentTimeMillis()
        return timestamp <= currentTime && sequence >= 0
    }
} 
