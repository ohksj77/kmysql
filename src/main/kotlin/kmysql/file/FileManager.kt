package kmysql.file

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
        if (isNew) dbDirectory.mkdirs()
        dbDirectory.list()?.forEach { filename ->
            if (filename.startsWith("temp")) {
                File(dbDirectory, filename).delete()
            }
        }
    }

    fun read(blockId: BlockId, page: Page) = withFileLock(blockId.filename) {
        try {
            val f = getOrOpenFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            f.channel.read(page.contents())
        } catch (e: IOException) {
            throw RuntimeException("cannot read block $blockId")
        }
    }

    fun write(blockId: BlockId, page: Page) = withFileLock(blockId.filename) {
        try {
            val f = getOrOpenFile(blockId.filename)
            f.seek((blockId.number * blockSize).toLong())
            f.channel.write(page.contents())
        } catch (e: IOException) {
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
            blockId
        } catch (e: IOException) {
            throw RuntimeException("cannot append block to $filename")
        }
    }

    fun length(filename: String): Int {
        try {
            val f = getOrOpenFile(filename)
            return (f.length() / blockSize).toInt()
        } catch (e: IOException) {
            throw RuntimeException("cannot access $filename")
        }
    }

    private fun getOrOpenFile(filename: String): RandomAccessFile =
        openFiles.computeIfAbsent(filename) {
            val dbTable = File(dbDirectory, filename)
            RandomAccessFile(dbTable, "rws")
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
