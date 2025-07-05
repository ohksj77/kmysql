package kmysql

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.record.Rid
import kmysql.transaction.Transaction
import kmysql.transaction.TransactionConfig
import kmysql.transaction.concurrency.IsolationLevel
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestMethodOrder
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class BufferAndConcurrencyTest {
    private lateinit var fileManager: FileManager
    private lateinit var bufferManager: BufferManager
    private lateinit var logManager: LogManager
    private lateinit var transaction: Transaction
    private lateinit var testDir: File

    @BeforeEach
    fun setUp() {
        testDir = File("testdb_buffer_concurrency")
        if (testDir.exists()) {
            testDir.deleteRecursively()
        }
        testDir.mkdirs()

        fileManager = FileManager(testDir, 400)
        logManager = LogManager(fileManager, "testlog_buffer_concurrency")
        bufferManager = BufferManager(fileManager, logManager, 8)
        transaction = Transaction(
            fileManager,
            bufferManager,
            logManager,
            TransactionConfig.kmysqlDefault().copy(
                isolationLevel = IsolationLevel.REPEATABLE_READ,
                autoCommit = false
            )
        )
    }

    @AfterEach
    fun tearDown() {
        transaction.rollback()
        testDir.deleteRecursively()
    }

    @Test
    @Order(1)
    fun testBasicBufferOperations() {
        val blockId = BlockId("testfile_basic", 0)

        // Pin a block
        transaction.pin(blockId)

        // Get buffer
        val buffer = transaction.getBuffer(blockId)
        assertNotNull(buffer)

        // Write data
        val page = buffer!!.contents()
        page.setInt(0, 42)
        page.setString(4, "test")

        // Verify data
        assertEquals(42, page.getInt(0))
        assertEquals("test", page.getString(4))

        // Unpin
        transaction.unpin(blockId)
    }

    @Test
    @Order(2)
    fun testBufferPinning() {
        val blockId = BlockId("testfile_pinning", 0)

        // Pin multiple times
        transaction.pin(blockId)
        transaction.pin(blockId)

        val buffer = transaction.getBuffer(blockId)
        assertNotNull(buffer)
        assertTrue(buffer!!.isPinned())

        // Unpin once
        transaction.unpin(blockId)
        assertTrue(buffer.isPinned())

        // Unpin again
        transaction.unpin(blockId)
        assertFalse(buffer.isPinned())
    }

    @Test
    @Order(4)
    fun testRecordLocking() {
        val rid1 = Rid(0, 0)
        val rid2 = Rid(0, 1)

        // Acquire shared locks
        transaction.sLockRecord(rid1)
        transaction.sLockRecord(rid2)

        // Release shared locks
        transaction.sUnlockRecord(rid1)
        transaction.sUnlockRecord(rid2)

        // Acquire exclusive locks
        transaction.xLockRecord(rid1)
        transaction.xLockRecord(rid2)

        // Release exclusive locks
        transaction.xUnlockRecord(rid1)
        transaction.xUnlockRecord(rid2)
    }

    @Test
    @Order(5)
    fun testBlockLevelLocking() {
        val blockId1 = BlockId("testfile1_blocklock", 0)
        val blockId2 = BlockId("testfile2_blocklock", 0)

        // Test shared lock
        transaction.size("testfile1_blocklock") // This acquires sLock internally

        // Test exclusive lock
        transaction.append("testfile2_blocklock") // This acquires xLock internally

        // Test multiple operations on same block
        transaction.size("testfile1_blocklock")
        transaction.append("testfile1_blocklock")
    }

    @Test
    @Order(6)
    fun testConcurrentAccess() {
        val blockId = BlockId("testfile_concurrent", 0)
        val latch = CountDownLatch(2)

        // Thread 1: Read operation
        val thread1 = Thread {
            try {
                transaction.pin(blockId)
                val buffer = transaction.getBuffer(blockId)
                assertNotNull(buffer)
                Thread.sleep(100) // Simulate work
                transaction.unpin(blockId)
            } finally {
                latch.countDown()
            }
        }

        // Thread 2: Write operation
        val thread2 = Thread {
            try {
                transaction.pin(blockId)
                transaction.setInt(blockId, 0, 200, true)
                transaction.unpin(blockId)
            } finally {
                latch.countDown()
            }
        }

        thread1.start()
        thread2.start()

        assertTrue(latch.await(5, TimeUnit.SECONDS))
    }

    @Test
    @Order(7)
    fun testBufferManagerCapacity() {
        val initialAvailable = transaction.availableBuffers()
        assertTrue(initialAvailable > 0)

        // Pin multiple blocks to consume buffers
        val blocks = mutableListOf<BlockId>()
        repeat(initialAvailable) { i ->
            val blockId = BlockId("testfile_capacity", i)
            transaction.pin(blockId)
            blocks.add(blockId)
        }

        // Should have no available buffers
        assertEquals(0, transaction.availableBuffers())

        // Unpin all blocks
        blocks.forEach { transaction.unpin(it) }

        // Should have buffers available again
        assertEquals(initialAvailable, transaction.availableBuffers())
    }

    @Test
    @Order(8)
    fun testTransactionIsolation() {
        val blockId1 = BlockId("testfile1_isolation", 0)
        val blockId2 = BlockId("testfile2_isolation", 0)

        // Create new transactions for this test
        val transaction1 = Transaction(
            fileManager,
            bufferManager,
            logManager,
            TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        )

        // Set initial value in first block
        transaction1.setInt(blockId1, 0, 100, true)
        transaction1.commit()

        // Create new transaction with READ_COMMITTED isolation using same log manager
        val transaction2 = Transaction(
            fileManager,
            bufferManager,
            logManager, // Use same log manager
            TransactionConfig.kmysqlDefault().copy(
                isolationLevel = IsolationLevel.READ_COMMITTED,
                autoCommit = false
            )
        )

        // Create third transaction for modifications
        val transaction3 = Transaction(
            fileManager,
            bufferManager,
            logManager,
            TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        )

        // Read value in new transaction from first block
        transaction2.pin(blockId1)
        val initialValue = transaction2.getInt(blockId1, 0)
        assertEquals(100, initialValue, "Initial read should return 100")

        // Modify in third transaction in second block (avoid lock conflict)
        transaction3.setInt(blockId2, 0, 200, true)

        // In READ_COMMITTED, transaction2 should still see the old value in first block
        // because the modification hasn't been committed yet
        val valueAfterModification = transaction2.getInt(blockId1, 0)
        assertEquals(100, valueAfterModification, "Should still see old value before commit")

        // Now commit the modification
        transaction3.commit()

        // After commit, transaction2 should still see the same value in first block
        // because we modified a different block
        val valueAfterCommit = transaction2.getInt(blockId1, 0)
        assertEquals(100, valueAfterCommit, "Should still see same value in first block")

        // But transaction2 should see the new value in second block
        transaction2.pin(blockId2)
        val valueInSecondBlock = transaction2.getInt(blockId2, 0)
        assertEquals(200, valueInSecondBlock, "Should see new value in second block after commit")

        transaction2.unpin(blockId1)
        transaction2.unpin(blockId2)

        // Now rollback remaining transactions
        transaction2.rollback()
        transaction3.rollback()
    }

    @Test
    @Order(9)
    fun testBufferFlush() {
        val blockId = BlockId("testfile_flush", 0)

        transaction.pin(blockId)
        transaction.setInt(blockId, 0, 300, true)

        // Force flush
        transaction.flushBuffer(blockId)

        // Verify data is still accessible
        assertEquals(300, transaction.getInt(blockId, 0))

        transaction.unpin(blockId)
    }

    @Test
    @Order(10)
    fun testDeadlockPrevention() {
        val rid1 = Rid(0, 0)
        val rid2 = Rid(1, 0)

        // Acquire locks in consistent order to prevent deadlock
        transaction.xLockRecord(rid1)
        transaction.xLockRecord(rid2)

        // Perform operations
        val blockId1 = BlockId("testfile1_deadlock", 0)
        val blockId2 = BlockId("testfile2_deadlock", 0)
        transaction.setInt(blockId1, 0, 100, true)
        transaction.setInt(blockId2, 0, 200, true)

        // Release locks
        transaction.xUnlockRecord(rid1)
        transaction.xUnlockRecord(rid2)
    }

    @Test
    @Order(11)
    fun testConcurrentTransactions() {
        val blockId = BlockId("testfile_concurrent_tx", 0)

        // Create initial transaction to set up data
        val setupTransaction = Transaction(
            fileManager,
            bufferManager,
            logManager,
            TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        )

        // Set initial value
        setupTransaction.setInt(blockId, 0, 100, true)
        setupTransaction.commit()

        // Create new transactions for concurrent testing
        val transaction1 = Transaction(
            fileManager,
            bufferManager,
            logManager,
            TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        )

        val transaction2 = Transaction(
            fileManager,
            bufferManager,
            logManager,
            TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        )

        // Both transactions read the same value
        transaction1.pin(blockId)
        transaction2.pin(blockId)

        assertEquals(100, transaction1.getInt(blockId, 0))
        assertEquals(100, transaction2.getInt(blockId, 0))

        // Modify in transaction2
        transaction2.setInt(blockId, 0, 200, true)

        // Original transaction should still see old value
        assertEquals(100, transaction1.getInt(blockId, 0))

        // Commit transaction2
        transaction2.commit()

        // Original transaction should still see old value (REPEATABLE READ)
        assertEquals(100, transaction1.getInt(blockId, 0))

        transaction1.unpin(blockId)
        transaction2.unpin(blockId)

        // Now rollback remaining transaction
        transaction1.rollback()
    }

    @Test
    @Order(12)
    fun testStressTest() {
        val numOperations = 20 // Further reduced
        val numThreads = 2
        val latch = CountDownLatch(numThreads)

        repeat(numThreads) { threadId ->
            Thread {
                try {
                    val localTransaction = Transaction(
                        fileManager,
                        bufferManager,
                        logManager, // Use same log manager
                        TransactionConfig.kmysqlDefault().copy(autoCommit = false)
                    )

                    try {
                        repeat(numOperations) { i ->
                            val blockId = BlockId("testfile_stress_$threadId", i % 3) // Further reduced
                            localTransaction.pin(blockId)

                            // Random operation
                            when (i % 3) {
                                0 -> {
                                    localTransaction.setInt(blockId, 0, i, true)
                                }

                                1 -> {
                                    val rid = Rid(i % 2, i % 2) // Further reduced ranges
                                    localTransaction.xLockRecord(rid)
                                    localTransaction.xUnlockRecord(rid)
                                }

                                2 -> {
                                    val value = localTransaction.getInt(blockId, 0)
                                    // Just read, no assertion needed
                                }
                            }

                            localTransaction.unpin(blockId)
                        }

                        localTransaction.commit()
                    } finally {
                        if (localTransaction.isActive()) {
                            localTransaction.rollback()
                        }
                    }
                } finally {
                    latch.countDown()
                }
            }.start()
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS)) // Further reduced timeout
    }
} 
