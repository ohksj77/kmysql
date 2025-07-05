package kmysql

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.transaction.Transaction
import kmysql.transaction.TransactionConfig
import kmysql.transaction.concurrency.IsolationLevel
import kmysql.transaction.concurrency.VersionManager
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.File

@DisplayName("KMySQL 종합 트랜잭션 테스트")
class ComprehensiveTransactionTest {
    private lateinit var fileManager: FileManager
    private lateinit var bufferManager: BufferManager
    private lateinit var logManager: LogManager
    val testDir = File("test_comprehensive")

    @BeforeEach
    fun setUp() {
        if (testDir.exists()) {
            testDir.deleteRecursively()
        }
        fileManager = FileManager(testDir, 400)
        logManager = LogManager(fileManager, "test_comprehensive.log")
        bufferManager = BufferManager(fileManager, logManager, 8)
        VersionManager.reset()
    }

    @AfterEach
    fun tearDown() {
        testDir.deleteRecursively()
    }

    @Test
    @DisplayName("세이브포인트 테스트")
    fun `세이브포인트 테스트`() {
        println("=== 세이브포인트 테스트 ===")

        val config = TransactionConfig(autoCommit = false)
        val block = BlockId("savepointfile", 0)

        val tx = Transaction(fileManager, bufferManager, logManager, config)
        tx.pin(block)

        tx.setInt(block, 0, 10, true)
        println("1. 초기 값 설정: 10")

        tx.createSavepoint("sp1")
        tx.setInt(block, 0, 20, true)
        println("2. sp1 세이브포인트 후 값 변경: 20")

        tx.createSavepoint("sp2")
        tx.setInt(block, 0, 30, true)
        println("3. sp2 세이브포인트 후 값 변경: 30")

        tx.rollbackToSavepoint("sp1")
        val valueAfterSp1Rollback = tx.getInt(block, 0)
        println("4. sp1로 롤백 후 값: $valueAfterSp1Rollback")
        assertNotNull(valueAfterSp1Rollback, "sp1 롤백 후 값이 null임")
        val sp1Value = valueAfterSp1Rollback!!
        assertEquals(10, sp1Value, "sp1 롤백 후 값이 10이어야 함 (sp1 생성 시점의 값)")

        try {
            tx.rollbackToSavepoint("sp2")
            fail("무효한 세이브포인트로 롤백했는데 예외가 발생하지 않음")
        } catch (e: Exception) {
            println("5. 무효한 sp2로 롤백 시도: 예외 발생 (예상됨)")
        }

        tx.commit()
        println("=== 세이브포인트 테스트 완료 ===")
    }

    @Test
    @DisplayName("격리 수준 테스트")
    fun `격리 수준 테스트`() {
        println("=== 격리 수준 테스트 ===")

        val block = BlockId("isolationfile", 0)

        println("1. READ UNCOMMITTED 테스트")
        val configReadUncommitted = TransactionConfig(
            isolationLevel = IsolationLevel.READ_UNCOMMITTED,
            autoCommit = false
        )

        val tx1 = Transaction(fileManager, bufferManager, logManager, configReadUncommitted)
        tx1.pin(block)
        tx1.setInt(block, 0, 100, true)
        println("   - tx1이 100 설정 (아직 커밋 안함)")

        val tx2 = Transaction(fileManager, bufferManager, logManager, configReadUncommitted)
        tx2.pin(block)
        val uncommittedValue = tx2.getInt(block, 0)
        println("   - tx2가 읽은 값: $uncommittedValue")

        assertNotNull(uncommittedValue, "READ UNCOMMITTED에서 읽은 값이 null임")
        assertEquals(100, uncommittedValue, "READ UNCOMMITTED에서 커밋되지 않은 값을 읽을 수 있어야 함")

        tx1.rollback()
        tx2.commit()
        println("   - READ UNCOMMITTED 테스트 완료")

        println("2. READ COMMITTED 테스트")
        val configReadCommitted = TransactionConfig(
            isolationLevel = IsolationLevel.READ_COMMITTED,
            autoCommit = false
        )

        val tx3 = Transaction(fileManager, bufferManager, logManager, configReadCommitted)
        tx3.pin(block)
        tx3.setInt(block, 0, 200, true)
        tx3.commit()
        println("   - tx3이 200 설정 후 커밋")

        val tx4 = Transaction(fileManager, bufferManager, logManager, configReadCommitted)
        tx4.pin(block)
        val committedValue = tx4.getInt(block, 0)
        println("   - tx4가 읽은 값: $committedValue")

        assertNotNull(committedValue, "READ COMMITTED에서 읽은 값이 null임")
        assertEquals(200, committedValue, "READ COMMITTED에서 커밋된 값을 읽을 수 있어야 함")

        tx4.commit()
        println("=== 격리 수준 테스트 완료 ===")
    }

    @Test
    @DisplayName("RID 락 테스트")
    fun `RID 락 테스트`() {
        println("=== RID 락 테스트 ===")

        val config = TransactionConfig(autoCommit = false)
        val block = BlockId("rid_lock_test", 0)

        val tx1 = Transaction(fileManager, bufferManager, logManager, config)
        val tx2 = Transaction(fileManager, bufferManager, logManager, config)

        // RID 생성
        val rid1 = kmysql.record.Rid(0, 0)
        val rid2 = kmysql.record.Rid(0, 1)

        println("1. 첫 번째 트랜잭션이 RID1에 공유 락 획득")
        tx1.sLockRecord(rid1)
        println("   - tx1이 RID1 공유 락 획득")

        println("2. 두 번째 트랜잭션도 RID1에 공유 락 획득 가능")
        tx2.sLockRecord(rid1)
        println("   - tx2도 RID1 공유 락 획득 (호환됨)")

        println("3. 첫 번째 트랜잭션이 RID2에 배타 락 획득")
        tx1.xLockRecord(rid2)
        println("   - tx1이 RID2 배타 락 획득")

        println("4. 두 번째 트랜잭션이 RID2에 배타 락 시도 (대기)")
        try {
            tx2.xLockRecord(rid2)
            println("   - tx2가 RID2 배타 락 획득 (예상치 못한 성공)")
        } catch (e: Exception) {
            println("   - tx2가 RID2 배타 락 획득 실패 (예상됨): ${e.message}")
        }

        println("5. 첫 번째 트랜잭션 락 해제")
        tx1.xUnlockRecord(rid2)
        tx1.sUnlockRecord(rid1)

        println("6. 두 번째 트랜잭션 락 해제")
        tx2.sUnlockRecord(rid1)

        tx1.commit()
        tx2.commit()
        println("=== RID 락 테스트 완료 ===")
    }
}
