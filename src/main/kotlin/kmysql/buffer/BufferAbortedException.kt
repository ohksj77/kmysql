package kmysql.buffer

class BufferAbortException(message: String) : RuntimeException() {

    companion object {
        fun interrupted(e: InterruptedException) =
            BufferAbortException(e.message ?: "Buffer operation was interrupted")

        fun timeout() =
            BufferAbortException("Buffer operation timed out")
    }
}
