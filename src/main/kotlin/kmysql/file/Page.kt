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
        return byteBuffer.getInt(offset)
    }

    fun setInt(offset: Int, n: Int) {
        byteBuffer.putInt(offset, n)
    }

    fun getBytes(offset: Int): ByteArray {
        byteBuffer.position(offset)
        val length = byteBuffer.int
        val b = ByteArray(length)
        byteBuffer.get(b)
        return b
    }

    fun setBytes(offset: Int, bytes: ByteArray) {
        byteBuffer.position(offset)
        byteBuffer.putInt(bytes.size)
        byteBuffer.put(bytes)
    }

    fun getString(offset: Int): String {
        val b = getBytes(offset)
        return String(b, charset)
    }

    fun setString(offset: Int, s: String) {
        val b = s.toByteArray(charset)
        setBytes(offset, b)
    }

    fun contents(): ByteBuffer {
        byteBuffer.position(0)
        return byteBuffer
    }

    companion object {
        private val charset = StandardCharsets.UTF_8

        fun maxLength(strSize: Int): Int {
            val bytesPerChar = charset.newEncoder().maxBytesPerChar()
            return Integer.BYTES + (strSize * (bytesPerChar.toInt()))
        }
    }
}
