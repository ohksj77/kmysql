package kmysql

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.transaction.Transaction
import kmysql.transaction.TransactionConfig
import kmysql.transaction.concurrency.IsolationLevel
import kmysql.transaction.concurrency.VersionManager
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransactionTest {
    private lateinit var fileManager: FileManager
    private lateinit var bufferManager: BufferManager
    private lateinit var logManager: LogManager

    @BeforeAll
    fun setup() {
        val testDir = File("testdb")
        if (testDir.exists()) {
            testDir.deleteRecursively()
        }
        fileManager = FileManager(testDir, 400)
        logManager = LogManager(fileManager, "test.log")
        bufferManager = BufferManager(fileManager, logManager, 8)

        // VersionManager 초기화
        VersionManager.reset()
    }

    @AfterAll
    fun cleanup() {
        File("testdb").deleteRecursively()
    }

    @Test
    fun `기본 트랜잭션 커밋 동작`() {
        val config = TransactionConfig.Companion.kmysqlDefault()
        val tx = Transaction(fileManager, bufferManager, logManager, config)
        val block = BlockId("testfile", 0)
        tx.pin(block)
        tx.setInt(block, 0, 42, true)
        tx.commit()

        val tx2 = Transaction(fileManager, bufferManager, logManager, config)
        tx2.pin(block)
        val value = tx2.getInt(block, 0)
        Assertions.assertEquals(42, value)
        tx2.commit()
    }

    @Test
    fun `트랜잭션 롤백 동작`() {
        val config = TransactionConfig(autoCommit = false)
        val tx = Transaction(fileManager, bufferManager, logManager, config)
        val block = BlockId("testfile", 10)
        tx.pin(block)
        tx.setInt(block, 0, 100, true)
        tx.rollback()

        val tx2 = Transaction(fileManager, bufferManager, logManager, config)
        tx2.pin(block)
        val value = tx2.getInt(block, 0)
        Assertions.assertNull(value)
        tx2.commit()
    }

    @Test
    fun `Savepoint 생성, 롤백, 해제`() {
        val config = TransactionConfig(autoCommit = false)
        val tx = Transaction(fileManager, bufferManager, logManager, config)
        val block = BlockId("testfile", 11)
        tx.pin(block)
        tx.setInt(block, 0, 1, true)
        val sp1 = tx.createSavepoint("sp1")
        tx.setInt(block, 0, 2, true)
        tx.rollbackToSavepoint(sp1)
        val value = tx.getInt(block, 0)
        Assertions.assertEquals(1, value)
        tx.releaseSavepoint(sp1)
        tx.commit()
    }

    @Test
    fun `Auto-commit 모드 동작`() {
        val config = TransactionConfig(autoCommit = true)
        val tx = Transaction(fileManager, bufferManager, logManager, config)
        val block = BlockId("testfile", 12)
        tx.pin(block)
        tx.setInt(block, 0, 77, true)

        val tx2 = Transaction(fileManager, bufferManager, logManager)
        tx2.pin(block)
        val value = tx2.getInt(block, 0)
        Assertions.assertEquals(77, value)
        tx2.commit()
    }

    @Test
    fun `격리 수준 READ UNCOMMITTED - Dirty Read 허용`() {
        val config1 = TransactionConfig(isolationLevel = IsolationLevel.READ_UNCOMMITTED)
        val config2 = TransactionConfig(isolationLevel = IsolationLevel.READ_UNCOMMITTED)
        val block = BlockId("testfile", 13)

        val tx1 = Transaction(fileManager, bufferManager, logManager, config1)
        val tx2 = Transaction(fileManager, bufferManager, logManager, config2)
        tx1.pin(block)
        tx2.pin(block)
        tx1.setInt(block, 0, 123, true)
        val value = tx2.getInt(block, 0)
        Assertions.assertEquals(123, value)
        tx1.rollback()
        tx2.commit()
    }

    @Test
    fun `격리 수준 READ COMMITTED - Dirty Read 방지`() {
        val config1 = TransactionConfig(isolationLevel = IsolationLevel.READ_COMMITTED, autoCommit = false)
        val config2 = TransactionConfig(isolationLevel = IsolationLevel.READ_COMMITTED, autoCommit = false)
        val block = BlockId("testfile", 14)

        val tx1 = Transaction(fileManager, bufferManager, logManager, config1)
        val tx2 = Transaction(fileManager, bufferManager, logManager, config2)
        tx1.pin(block)
        tx2.pin(block)
        tx1.setInt(block, 0, 456, true)
        val value = tx2.getInt(block, 0)
        Assertions.assertNull(value)
        tx1.commit()
        val value2 = tx2.getInt(block, 0)
        Assertions.assertEquals(456, value2)
        tx2.commit()
    }

    @Test
    fun `격리 수준 REPEATABLE READ - Non-Repeatable Read 방지`() {
        val config1 = TransactionConfig(isolationLevel = IsolationLevel.REPEATABLE_READ)
        val config2 = TransactionConfig(isolationLevel = IsolationLevel.REPEATABLE_READ)
        val block = BlockId("testfile", 15)

        val tx1 = Transaction(fileManager, bufferManager, logManager, config1)
        val tx2 = Transaction(fileManager, bufferManager, logManager, config2)
        tx1.pin(block)
        tx2.pin(block)
        tx1.setInt(block, 0, 789, true)
        tx1.commit()
        val v1 = tx2.getInt(block, 0)

        // 새로운 트랜잭션으로 변경
        val tx3 = Transaction(fileManager, bufferManager, logManager, config1)
        tx3.pin(block)
        tx3.setInt(block, 0, 999, true)
        tx3.commit()
        val v2 = tx2.getInt(block, 0)
        Assertions.assertEquals(v1, v2)
        tx2.commit()
    }

    @Test
    fun `격리 수준 SERIALIZABLE - Phantom Read 방지`() {
        val config1 = TransactionConfig(isolationLevel = IsolationLevel.SERIALIZABLE)
        val config2 = TransactionConfig(isolationLevel = IsolationLevel.SERIALIZABLE)
        val block = BlockId("testfile", 16)

        val tx1 = Transaction(fileManager, bufferManager, logManager, config1)
        val tx2 = Transaction(fileManager, bufferManager, logManager, config2)
        tx1.pin(block)
        tx2.pin(block)
        tx1.setInt(block, 0, 111, true)
        tx1.commit()
        val v1 = tx2.getInt(block, 0)

        // 새로운 트랜잭션으로 변경
        val tx3 = Transaction(fileManager, bufferManager, logManager, config1)
        tx3.pin(block)
        tx3.setInt(block, 0, 222, true)
        tx3.commit()
        val v2 = tx2.getInt(block, 0)
        Assertions.assertEquals(v1, v2)
        tx2.commit()
    }

    @Test
    fun `롤백만 테스트`() {
        val config = TransactionConfig(autoCommit = false)
        val tx = Transaction(fileManager, bufferManager, logManager, config)
        val block = BlockId("testfile", 17)
        tx.pin(block)
        tx.setInt(block, 0, 100, true)
        tx.rollback()

        val tx2 = Transaction(fileManager, bufferManager, logManager, config)
        tx2.pin(block)
        val value = tx2.getInt(block, 0)
        Assertions.assertNull(value)
        tx2.commit()
    }
}
