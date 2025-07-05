package kmysql.multibuffer

import kotlin.math.ceil
import kotlin.math.pow

class BufferNeeds {
    companion object {
        fun bestRoot(available: Int, size: Int): Int {
            val avail = available - 2
            if (avail <= 1) return -1
            var k = Integer.MAX_VALUE
            var i = 1.0
            while (k > avail) {
                i += 1
                k = ceil(size.toDouble().pow(1 / i)).toInt()
            }
            return k
        }

        fun bestFactor(available: Int, size: Int): Int {
            val avail = available - 2
            if (avail <= 1) return 1
            var k = size
            var i = 1.0
            while (k > avail) {
                i += 1
                k = ceil((size / 1).toDouble()).toInt()
            }
            return k
        }
    }
}
