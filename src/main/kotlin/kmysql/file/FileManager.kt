package kmysql.file

import kmysql.util.ConsoleLogger
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

class FileManager(
    val dbDirectory: File,
    val blockSize: Int,
) {
    var isNew: Boolean = !dbDirectory.exists()
    private val openFiles = ConcurrentHashMap<String, RandomAccessFile>()
    private val fileLocks = ConcurrentHashMap<String, ReentrantLock>()

    init {
        ConsoleLogger.info("FileManager 초기화: dbDirectory=${dbDirectory.absolutePath}, isNew=$isNew")
        if (isNew) {
            val created = dbDirectory.mkdirs()
            ConsoleLogger.info("데이터베이스 디렉토리 생성: $created")
        }
        dbDirectory.list()?.forEach { filename ->
            if (filename.startsWith("temp")) {
                val tempFile = File(dbDirectory, filename)
                val deleted = tempFile.delete()
                ConsoleLogger.info("임시 파일 삭제: $filename -> $deleted")
            }
        }
    }

    fun read(blockId: BlockId, page: Page) = withFileLock(blockId.filename) {
        try {
            val f = getOrOpenFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            val bytesRead = f.channel.read(page.contents())
            ConsoleLogger.info("파일 읽기: ${blockId.filename} 블록 ${blockId.number}, 읽은 바이트: $bytesRead")
        } catch (e: IOException) {
            ConsoleLogger.error("파일 읽기 실패: ${blockId.filename} 블록 ${blockId.number} - ${e.message}")
            throw RuntimeException("cannot read block $blockId")
        }
    }

    fun write(blockId: BlockId, page: Page) = withFileLock(blockId.filename) {
        try {
            val f = getOrOpenFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            val bytesWritten = f.channel.write(page.contents())
            ConsoleLogger.info("파일 쓰기: ${blockId.filename} 블록 ${blockId.number}, 쓴 바이트: $bytesWritten")
        } catch (e: IOException) {
            ConsoleLogger.error("파일 쓰기 실패: ${blockId.filename} 블록 ${blockId.number} - ${e.message}")
            throw RuntimeException("cannot write block $blockId")
        }
    }

    fun append(filename: String): BlockId = withFileLock(filename) {
        try {
            val newBlockNumber = length(filename)
            val blockId = BlockId(filename, newBlockNumber)
            val b = ByteArray(blockSize)
            val f = getOrOpenFile(filename)
            f.seek((blockId.number * blockSize).toLong())
            f.write(b)
            ConsoleLogger.info("파일 블록 추가: $filename 블록 $newBlockNumber, 파일 크기: ${f.length()} 바이트")
            blockId
        } catch (e: IOException) {
            ConsoleLogger.error("파일 블록 추가 실패: $filename - ${e.message}")
            throw RuntimeException("cannot append block to $filename")
        }
    }

    fun length(filename: String): Int {
        try {
            val f = getOrOpenFile(filename)
            val fileLength = f.length()
            val blockCount = (fileLength / blockSize).toInt()
            ConsoleLogger.info("파일 길이 조회: $filename, 파일 크기: ${fileLength} 바이트, 블록 수: $blockCount")
            return blockCount
        } catch (e: IOException) {
            ConsoleLogger.error("파일 길이 조회 실패: $filename - ${e.message}")
            throw RuntimeException("cannot access $filename")
        }
    }

    private fun getOrOpenFile(filename: String): RandomAccessFile =
        openFiles.computeIfAbsent(filename) {
            val dbTable = File(dbDirectory, filename)
            ConsoleLogger.info("파일 열기: ${dbTable.absolutePath}")
            try {
                RandomAccessFile(dbTable, "rws")
            } catch (e: IOException) {
                ConsoleLogger.error("파일 열기 실패: ${dbTable.absolutePath} - ${e.message}")
                throw RuntimeException("cannot open file $filename")
            }
        }

    fun sync() {
        openFiles.values.forEach { file ->
            try {
                file.fd.sync()
                ConsoleLogger.info("파일 동기화 완료: ${file.fd}")
            } catch (e: IOException) {
                ConsoleLogger.error("파일 동기화 실패: ${e.message}")
            }
        }
    }

    private inline fun <T> withFileLock(filename: String, action: () -> T): T {
        val lock = fileLocks.computeIfAbsent(filename) { ReentrantLock() }
        lock.lock()
        return try {
            action()
        } finally {
            lock.unlock()
        }
    }
}
