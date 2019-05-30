package com.pushtorefresh.storio3.sqlite.operations.execute

import androidx.annotation.WorkerThread
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.impl.ChainImpl.buildChain
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.queries.RawQuery
import kotlinx.coroutines.withContext

/**
 * Prepared Execute SQL Operation for [StorIOSQLite].
 */
class PreparedExecuteSQL internal constructor(
    private val storIOSQLite: StorIOSQLite,
    private val rawQuery: RawQuery
) : PreparedOperation<Any, RawQuery> {

    /**
     * Executes SQL Operation immediately in current thread.
     *
     *
     * Notice: This is blocking I/O operation that should not be executed on the Main Thread,
     * it can cause ANR (Activity Not Responding dialog), block the UI and drop animations frames.
     * So please, call this method on some background thread. See [WorkerThread].
     *
     * @return just a new instance of [Object], actually Execute SQL should return `void`,
     * but we can not return instance of [Void] so we just return [Object]
     * and you don't have to deal with `null`.
     */
    @WorkerThread
    override fun executeAsBlocking(): Any {
        return buildChain(storIOSQLite.interceptors(), RealCallInterceptor())
            .proceed(this)
    }

    override suspend fun await(): Any? {
        return withContext(storIOSQLite.defaultDispatcher()) {
            executeAsBlocking()
        }
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val lowLevel = storIOSQLite.lowLevel()
                lowLevel.executeSQL(rawQuery)

                val affectedTables = rawQuery.affectsTables()
                val affectedTags = rawQuery.affectsTags()

                if (!affectedTables.isEmpty() || !affectedTags.isEmpty()) {
                    lowLevel.notifyAboutChanges(Changes.newInstance(affectedTables, affectedTags))
                }


                return Any() as Result
            } catch (exception: Exception) {
                throw StorIOException("Error has occurred during ExecuteSQL operation. query = $rawQuery", exception)
            }

        }
    }

    override fun getData(): RawQuery {
        return rawQuery
    }

    /**
     * Builder for [PreparedExecuteSQL].
     */
    class Builder(private val storIOSQLite: StorIOSQLite) {

        /**
         * Required: Specifies query for ExecSql Operation.
         *
         * @param rawQuery any SQL query that you want to execute, but please, be careful with it.
         * Don't forget that you can set affected tables to the [RawQuery],
         * so ExecSQL operation will send notification about changes in that tables.
         * @return builder.
         */
        fun withQuery(rawQuery: RawQuery): CompleteBuilder {
            return CompleteBuilder(storIOSQLite, rawQuery)
        }
    }

    /**
     * Compile-time safe part of [Builder].
     */
    class CompleteBuilder internal constructor(private val storIOSQLite: StorIOSQLite, private val rawQuery: RawQuery) {

        /**
         * Prepares ExecSql Operation.
         *
         * @return [PreparedExecuteSQL] instance.
         */
        fun prepare(): PreparedExecuteSQL {
            return PreparedExecuteSQL(
                storIOSQLite,
                rawQuery
            )
        }
    }
}
