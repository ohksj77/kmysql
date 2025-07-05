package kmysql

import LogManager
import kmysql.buffer.BufferManager
import kmysql.file.BlockId
import kmysql.file.FileManager
import kmysql.transaction.Transaction
import kmysql.transaction.TransactionConfig
import kmysql.transaction.concurrency.IsolationLevel
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.File
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.system.measureTimeMillis

@DisplayName("KMySQL 성능 및 스트레스 테스트")
class PerformanceAndStressTest {
    private lateinit var fileManager: FileManager
    private lateinit var bufferManager: BufferManager
    private lateinit var logManager: LogManager
    private lateinit var testDir: File

    @BeforeEach
    fun setUp() {
        testDir = File("test_performance_stress")
        if (testDir.exists()) {
            testDir.deleteRecursively()
        }
        testDir.mkdirs()

        fileManager = FileManager(testDir, 800)
        logManager = LogManager(fileManager, "test_performance_stress.log")
        bufferManager = BufferManager(fileManager, logManager, 32)
    }

    @AfterEach
    fun tearDown() {
        testDir.deleteRecursively()
    }

    @Test
    @DisplayName("대량 데이터 삽입 성능 테스트")
    fun `대량 데이터 삽입 성능 테스트`() {
        println("=== 대량 데이터 삽입 성능 테스트 ===")

        val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        val numRecords = 150
        val block = BlockId("bulk_insert", 0)

        val tx = Transaction(fileManager, bufferManager, logManager, config)
        tx.pin(block)

        val insertionTime = measureTimeMillis {
            repeat(numRecords) { i ->
                val offset = i * 4  // 최대 offset: 150 * 4 = 600 bytes
                tx.setInt(block, offset, i, true)
            }
        }

        println("1. $numRecords records insertion time: ${insertionTime}ms")
        println("2. Average insertion time: ${insertionTime.toDouble() / numRecords}ms/record")

        // 데이터 검증
        repeat(numRecords) { i ->
            val value = tx.getInt(block, i * 4)
            assertEquals(i, value, "삽입된 값이 일치하지 않음: index=$i")
        }
        println("3. 데이터 검증 완료")

        tx.commit()
        println("=== 대량 데이터 삽입 성능 테스트 완료 ===")
    }

    @Test
    @DisplayName("대량 읽기 성능 테스트")
    fun `대량 읽기 성능 테스트`() {
        println("=== 대량 읽기 성능 테스트 ===")

        val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        // Page 크기를 고려하여 레코드 수 제한
        val numRecords = 150
        val block = BlockId("bulk_read", 0)

        // 초기 데이터 설정
        val tx1 = Transaction(fileManager, bufferManager, logManager, config)
        tx1.pin(block)
        repeat(numRecords) { i ->
            val offset = i * 4
            tx1.setInt(block, offset, i, true)
        }
        tx1.commit()
        println("1. $numRecords records initial setup completed")

        // 대량 읽기 성능 테스트
        val tx2 = Transaction(fileManager, bufferManager, logManager, config)
        tx2.pin(block)

        val readTime = measureTimeMillis {
            repeat(numRecords) { i ->
                val value = tx2.getInt(block, i * 4)
                assertEquals(i, value, "읽은 값이 일치하지 않음: index=$i")
            }
        }

        println("2. $numRecords records read time: ${readTime}ms")
        println("3. Average read time: ${readTime.toDouble() / numRecords}ms/record")

        tx2.commit()
        println("=== 대량 읽기 성능 테스트 완료 ===")
    }

    @Test
    @DisplayName("트랜잭션 처리량 테스트")
    fun `트랜잭션 처리량 테스트`() {
        println("=== 트랜잭션 처리량 테스트 ===")

        val numTransactions = 100
        // Page 크기를 고려하여 트랜잭션당 레코드 수 제한
        val recordsPerTransaction = 10
        val block = BlockId("throughput", 0)

        val totalTime = measureTimeMillis {
            repeat(numTransactions) { txId ->
                val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
                val tx = Transaction(fileManager, bufferManager, logManager, config)
                tx.pin(block)

                repeat(recordsPerTransaction) { recordId ->
                    val offset = (recordId % 150) * 4  // offset을 150 * 4 = 600 bytes로 제한
                    tx.setInt(block, offset, txId * 1000 + recordId, true)
                }

                tx.commit()
            }
        }

        val totalRecords = numTransactions * recordsPerTransaction
        val tps = numTransactions.toDouble() / (totalTime / 1000.0)
        val rps = totalRecords.toDouble() / (totalTime / 1000.0)

        println("1. 총 트랜잭션 수: $numTransactions")
        println("2. 트랜잭션당 레코드 수: $recordsPerTransaction")
        println("3. 총 레코드 수: $totalRecords")
        println("4. 총 처리 시간: ${totalTime}ms")
        println("5. 트랜잭션 처리량: ${String.format("%.2f", tps)} TPS")
        println("6. 레코드 처리량: ${String.format("%.2f", rps)} RPS")

        println("=== 트랜잭션 처리량 테스트 완료 ===")
    }

    @Test
    @DisplayName("동시 트랜잭션 스트레스 테스트")
    fun `동시 트랜잭션 스트레스 테스트`() {
        println("=== 동시 트랜잭션 스트레스 테스트 ===")

        val numThreads = 8
        val transactionsPerThread = 50
        // Page 크기를 고려하여 트랜잭션당 레코드 수 제한
        val recordsPerTransaction = 5
        val executor = Executors.newFixedThreadPool(numThreads)
        val latch = CountDownLatch(numThreads)

        val totalTime = measureTimeMillis {
            repeat(numThreads) { threadId ->
                executor.submit {
                    try {
                        repeat(transactionsPerThread) { txId ->
                            val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
                            val tx = Transaction(fileManager, bufferManager, logManager, config)

                            repeat(recordsPerTransaction) { recordId ->
                                val block = BlockId("stress_test", threadId)
                                tx.pin(block)
                                val offset = (recordId % 150) * 4  // offset을 150 * 4 = 600 bytes로 제한
                                tx.setInt(block, offset, threadId * 10000 + txId * 100 + recordId, true)
                            }

                            tx.commit()
                        }
                    } finally {
                        latch.countDown()
                    }
                }
            }

            latch.await(30, TimeUnit.SECONDS)
        }

        executor.shutdown()

        val totalTransactions = numThreads * transactionsPerThread
        val totalRecords = totalTransactions * recordsPerTransaction
        val tps = totalTransactions.toDouble() / (totalTime / 1000.0)
        val rps = totalRecords.toDouble() / (totalTime / 1000.0)

        println("1. Number of threads: $numThreads")
        println("2. Transactions per thread: $transactionsPerThread")
        println("3. Total transactions: $totalTransactions")
        println("4. Total records: $totalRecords")
        println("5. Total processing time: ${totalTime}ms")
        println("6. Transaction throughput: ${String.format("%.2f", tps)} TPS")
        println("7. Record throughput: ${String.format("%.2f", rps)} RPS")

        println("=== 동시 트랜잭션 스트레스 테스트 완료 ===")
    }

    @Test
    @DisplayName("버퍼 관리 성능 테스트")
    fun `버퍼 관리 성능 테스트`() {
        println("=== 버퍼 관리 성능 테스트 ===")

        val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        val numBlocks = 100
        // Page 크기를 고려하여 블록당 연산 수 제한
        val operationsPerBlock = 10

        val tx = Transaction(fileManager, bufferManager, logManager, config)

        val bufferTime = measureTimeMillis {
            repeat(numBlocks) { blockId ->
                val block = BlockId("buffer_test", blockId)
                tx.pin(block)

                repeat(operationsPerBlock) { opId ->
                    val offset = opId * 4  // 최대 offset: 10 * 4 = 40 bytes
                    tx.setInt(block, offset, blockId * 1000 + opId, true)
                }

                tx.unpin(block)
            }
        }

        val totalOperations = numBlocks * operationsPerBlock
        val opsPerSecond = totalOperations.toDouble() / (bufferTime / 1000.0)

        println("1. 블록 수: $numBlocks")
        println("2. 블록당 연산 수: $operationsPerBlock")
        println("3. 총 연산 수: $totalOperations")
        println("4. 총 처리 시간: ${bufferTime}ms")
        println("5. 연산 처리량: ${String.format("%.2f", opsPerSecond)} ops/sec")

        tx.commit()
        println("=== 버퍼 관리 성능 테스트 완료 ===")
    }

    @Test
    @DisplayName("격리 수준별 성능 테스트")
    fun `격리 수준별 성능 테스트`() {
        println("=== 격리 수준별 성능 테스트 ===")

        val isolationLevels = listOf(
            IsolationLevel.READ_UNCOMMITTED,
            IsolationLevel.READ_COMMITTED,
            IsolationLevel.REPEATABLE_READ,
            IsolationLevel.SERIALIZABLE
        )

        // Page 크기를 고려하여 연산 수 제한 (800 bytes / 4 bytes per int = 200 operations max)
        val numOperations = 150
        val block = BlockId("isolation_test", 0)

        isolationLevels.forEach { isolationLevel ->
            val config = TransactionConfig.kmysqlDefault().copy(
                isolationLevel = isolationLevel,
                autoCommit = false
            )

            val tx = Transaction(fileManager, bufferManager, logManager, config)
            tx.pin(block)

            val operationTime = measureTimeMillis {
                repeat(numOperations) { i ->
                    val offset = (i % 150) * 4  // offset을 150 * 4 = 600 bytes로 제한
                    tx.setInt(block, offset, i, true)
                    tx.getInt(block, offset)
                }
            }

            val opsPerSecond = numOperations.toDouble() / (operationTime / 1000.0)

            println("1. 격리 수준: ${isolationLevel.description}")
            println("2. 연산 수: $numOperations")
            println("3. 처리 시간: ${operationTime}ms")
            println("4. 처리량: ${String.format("%.2f", opsPerSecond)} ops/sec")

            tx.commit()
        }

        println("=== 격리 수준별 성능 테스트 완료 ===")
    }

    @Test
    @DisplayName("메모리 사용량 테스트")
    fun `메모리 사용량 테스트`() {
        println("=== 메모리 사용량 테스트 ===")

        val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        val numBlocks = 50
        val operationsPerBlock = 20

        val tx = Transaction(fileManager, bufferManager, logManager, config)

        val initialMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()

        repeat(numBlocks) { blockId ->
            val block = BlockId("memory_test", blockId)
            tx.pin(block)

            repeat(operationsPerBlock) { opId ->
                val offset = opId * 4
                tx.setInt(block, offset, blockId * 1000 + opId, true)
            }

            // 일부 블록은 언핀하여 버퍼 재사용 테스트
            if (blockId % 2 == 0) {
                tx.unpin(block)
            }
        }

        val finalMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()
        val memoryUsed = finalMemory - initialMemory

        println("1. 블록 수: $numBlocks")
        println("2. 블록당 연산 수: $operationsPerBlock")
        println("3. 초기 메모리: ${initialMemory / 1024}KB")
        println("4. 최종 메모리: ${finalMemory / 1024}KB")
        println("5. 사용된 메모리: ${memoryUsed / 1024}KB")

        tx.commit()
        println("=== 메모리 사용량 테스트 완료 ===")
    }

    @Test
    @DisplayName("장기 실행 스트레스 테스트")
    fun `장기 실행 스트레스 테스트`() {
        println("=== 장기 실행 스트레스 테스트 ===")

        val numThreads = 4
        val durationSeconds = 10
        val executor = Executors.newFixedThreadPool(numThreads)
        val latch = CountDownLatch(numThreads)
        val startTime = System.currentTimeMillis()

        var totalTransactions = 0L
        var totalOperations = 0L

        repeat(numThreads) { threadId ->
            executor.submit {
                try {
                    while (System.currentTimeMillis() - startTime < durationSeconds * 1000) {
                        val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
                        val tx = Transaction(fileManager, bufferManager, logManager, config)

                        repeat(5) { opId ->
                            val block = BlockId("long_stress", threadId)
                            tx.pin(block)
                            tx.setInt(block, opId * 4, (System.currentTimeMillis() % 10000).toInt(), true)
                            tx.getInt(block, opId * 4)
                            tx.unpin(block)
                            totalOperations++
                        }

                        tx.commit()
                        totalTransactions++
                    }
                } finally {
                    latch.countDown()
                }
            }
        }

        latch.await((durationSeconds + 5).toLong(), TimeUnit.SECONDS)
        executor.shutdown()

        val actualDuration = (System.currentTimeMillis() - startTime) / 1000.0
        val tps = totalTransactions / actualDuration
        val ops = totalOperations / actualDuration

        println("1. 실행 시간: ${String.format("%.2f", actualDuration)}초")
        println("2. 총 트랜잭션 수: $totalTransactions")
        println("3. 총 연산 수: $totalOperations")
        println("4. 트랜잭션 처리량: ${String.format("%.2f", tps)} TPS")
        println("5. 연산 처리량: ${String.format("%.2f", ops)} ops/sec")

        println("=== 장기 실행 스트레스 테스트 완료 ===")
    }

    @Test
    @DisplayName("복구 성능 테스트")
    fun `복구 성능 테스트`() {
        println("=== 복구 성능 테스트 ===")

        val config = TransactionConfig.kmysqlDefault().copy(autoCommit = false)
        val numTransactions = 100
        // Page 크기를 고려하여 트랜잭션당 연산 수 제한
        val operationsPerTransaction = 10

        // 초기 데이터 설정
        repeat(numTransactions) { txId ->
            val tx = Transaction(fileManager, bufferManager, logManager, config)
            val block = BlockId("recovery_test", txId)
            tx.pin(block)

            repeat(operationsPerTransaction) { opId ->
                val offset = opId * 4  // 최대 offset: 10 * 4 = 40 bytes
                tx.setInt(block, offset, txId * 1000 + opId, true)
            }

            tx.commit()
        }

        println("1. $numTransactions transactions initial setup completed")

        // 복구 성능 테스트
        val recoveryTime = measureTimeMillis {
            val recoveryTx = Transaction(fileManager, bufferManager, logManager, config)
            recoveryTx.recover()
        }

        println("2. Recovery time: ${recoveryTime}ms")
        println(
            "3. Recovery throughput: ${
                String.format(
                    "%.2f",
                    numTransactions.toDouble() / (recoveryTime / 1000.0)
                )
            } TPS"
        )

        println("=== 복구 성능 테스트 완료 ===")
    }
}
