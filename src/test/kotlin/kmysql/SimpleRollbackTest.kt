package kmysql

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.transaction.Transaction
import kmysql.transaction.TransactionConfig
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SimpleRollbackTest {
    private lateinit var fileManager: FileManager
    private lateinit var bufferManager: BufferManager
    private lateinit var logManager: LogManager

    @BeforeAll
    fun setup() {
        val testDir = File("testdb_simple")
        if (testDir.exists()) {
            testDir.deleteRecursively()
        }
        fileManager = FileManager(testDir, 400)
        logManager = LogManager(fileManager, "test_simple.log")
        bufferManager = BufferManager(fileManager, logManager, 4)
    }

    @AfterAll
    fun cleanup() {
        File("testdb_simple").deleteRecursively()
    }

    @Test
    fun `롤백 테스트`() {
        println("1. 트랜잭션 생성 및 값 설정")
        val config = TransactionConfig(autoCommit = false)
        val tx = Transaction(fileManager, bufferManager, logManager, config)
        val block = BlockId("testfile", 20)  // 고유한 블록 번호 사용
        tx.pin(block)
        tx.setInt(block, 0, 100, true)
        println("   - setInt(100) 완료")

        println("2. 롤백 실행")
        tx.rollback()
        println("   - rollback() 완료")

        println("3. 새로운 트랜잭션으로 값 확인")
        val tx2 = Transaction(fileManager, bufferManager, logManager, config)
        tx2.pin(block)
        val value = tx2.getInt(block, 0)
        println("   - getInt 결과: $value")

        // 4. 검증
        println("4. 검증")
        Assertions.assertNull(value, "getInt 결과가 null이어야 함 (롤백 후)")
        println("   - 검증 통과!")

        tx2.commit()
    }
} 
