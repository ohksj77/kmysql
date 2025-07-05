package kmysql.file

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class Page {
    private var byteBuffer: ByteBuffer
    private val charset = StandardCharsets.UTF_8

    constructor(blockSize: Int) {
        byteBuffer = ByteBuffer.allocateDirect(blockSize)
    }

    constructor(bytes: ByteArray) {
        byteBuffer = ByteBuffer.wrap(bytes)
    }

    fun getInt(offset: Int): Int {
        if (offset < 0 || offset + Integer.BYTES > byteBuffer.capacity()) {
            throw IllegalArgumentException("Invalid offset: $offset, buffer capacity: ${byteBuffer.capacity()}")
        }
        return byteBuffer.getInt(offset)
    }

    fun setInt(offset: Int, n: Int) {
        if (offset < 0 || offset + Integer.BYTES > byteBuffer.capacity()) {
            throw IllegalArgumentException("Invalid offset: $offset, buffer capacity: ${byteBuffer.capacity()}")
        }
        byteBuffer.putInt(offset, n)
    }

    fun getBytes(offset: Int): ByteArray {
        if (offset < 0 || offset + Integer.BYTES > byteBuffer.capacity()) {
            throw IllegalArgumentException("Invalid offset: $offset, buffer capacity: ${byteBuffer.capacity()}")
        }
        val originalPosition = byteBuffer.position()
        byteBuffer.position(offset)
        val length = byteBuffer.int
        if (length < 0 || offset + Integer.BYTES + length > byteBuffer.capacity()) {
            throw IllegalArgumentException("Invalid length: $length at offset: $offset")
        }
        val b = ByteArray(length)
        byteBuffer.get(b)
        byteBuffer.position(originalPosition)
        return b
    }

    fun setBytes(offset: Int, bytes: ByteArray) {
        if (offset < 0 || offset + Integer.BYTES + bytes.size > byteBuffer.capacity()) {
            throw IllegalArgumentException("Invalid offset: $offset or bytes size: ${bytes.size}, buffer capacity: ${byteBuffer.capacity()}")
        }
        val originalPosition = byteBuffer.position()
        byteBuffer.position(offset)
        byteBuffer.putInt(bytes.size)
        byteBuffer.put(bytes)
        byteBuffer.position(originalPosition)
    }

    fun getString(offset: Int): String {
        val b = getBytes(offset)
        val result = String(b, charset)
        return result
    }

    fun setString(offset: Int, s: String) {
        val b = s.toByteArray(charset)
        setBytes(offset, b)
    }

    fun contents(): ByteBuffer {
        byteBuffer.position(0)
        return byteBuffer
    }

    fun toByteArray(): ByteArray {
        byteBuffer.position(0)
        val bytes = ByteArray(byteBuffer.capacity())
        byteBuffer.get(bytes)
        return bytes
    }

    fun setLong(offset: Int, value: Long) {
        byteBuffer.position(offset)
        byteBuffer.putLong(value)
    }

    fun getLong(offset: Int): Long {
        byteBuffer.position(offset)
        return byteBuffer.long
    }

    companion object {
        private val charset = StandardCharsets.UTF_8

        fun maxLength(strSize: Int): Int {
            val bytesPerChar = charset.newEncoder().maxBytesPerChar()
            return Integer.BYTES + (strSize * (bytesPerChar.toInt()))
        }
    }
}
