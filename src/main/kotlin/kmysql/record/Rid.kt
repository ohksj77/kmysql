package kmysql.record

import java.util.*

class Rid(
    val blockNumber: Int,
    val slot: Int
) {
    override fun equals(other: Any?): Boolean {
        val r = other as Rid
        return blockNumber == r.blockNumber && slot == r.slot
    }

    override fun toString(): String {
        return "[$blockNumber, $slot]"
    }

    override fun hashCode(): Int {
        return Objects.hash(blockNumber, slot)
    }
}
