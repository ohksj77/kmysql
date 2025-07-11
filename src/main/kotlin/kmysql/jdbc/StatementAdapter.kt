package kmysql.jdbc

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.Statement

abstract class StatementAdapter : Statement {
    override fun addBatch(sql: String?) {
        throw SQLException("operation not implemented")
    }

    override fun cancel() {
        throw SQLException("operation not implemented")
    }

    override fun clearWarnings() {
        throw SQLException("operation not implemented")
    }

    override fun clearBatch() {
        throw SQLException("operation not implemented")
    }

    override fun closeOnCompletion() {
        throw SQLException("operation not implemented")
    }

    override fun close() {
        throw SQLException("operation not implemented")
    }

    override fun execute(sql: String?): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun execute(sql: String?, autoGeneratedKeys: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun execute(sql: String?, columnIndexes: IntArray?): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun execute(sql: String?, columnNames: Array<out String>?): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun executeBatch(): IntArray {
        throw SQLException("operation not implemented")
    }

    override fun executeQuery(sql: String?): ResultSet {
        throw SQLException("operation not implemented")
    }

    override fun executeUpdate(sql: String?): Int {
        throw SQLException("operation not implemented")
    }

    override fun executeUpdate(sql: String?, autoGeneratedKeys: Int): Int {
        throw SQLException("operation not implemented")
    }

    override fun executeUpdate(sql: String?, columnIndexes: IntArray?): Int {
        throw SQLException("operation not implemented")
    }

    override fun executeUpdate(sql: String?, columnNames: Array<out String>?): Int {
        throw SQLException("operation not implemented")
    }

    abstract override fun getConnection(): Connection

    override fun getFetchDirection(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getFetchSize(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getGeneratedKeys(): ResultSet {
        throw SQLException("operation not implemented")
    }

    override fun getMaxFieldSize(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getMaxRows(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getMoreResults(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun getMoreResults(current: Int): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun getQueryTimeout(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getResultSet(): ResultSet {
        throw SQLException("operation not implemented")
    }

    override fun getResultSetConcurrency(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getResultSetHoldability(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getResultSetType(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getUpdateCount(): Int {
        throw SQLException("operation not implemented")
    }

    override fun getWarnings(): SQLWarning {
        throw SQLException("operation not implemented")
    }

    override fun isClosed(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isCloseOnCompletion(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isPoolable(): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun isWrapperFor(iface: Class<*>?): Boolean {
        throw SQLException("operation not implemented")
    }

    override fun setCursorName(name: String?) {
        throw SQLException("operation not implemented")
    }

    override fun setEscapeProcessing(enable: Boolean) {
        throw SQLException("operation not implemented")
    }

    override fun setFetchDirection(direction: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setFetchSize(rows: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setMaxFieldSize(max: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setMaxRows(max: Int) {
        throw SQLException("operation not implemented")
    }

    override fun setPoolable(poolable: Boolean) {
        throw SQLException("operation not implemented")
    }

    override fun setQueryTimeout(seconds: Int) {
        throw SQLException("operation not implemented")
    }

    override fun <T : Any?> unwrap(iface: Class<T>?): T {
        throw SQLException("operation not implemented")
    }
}
