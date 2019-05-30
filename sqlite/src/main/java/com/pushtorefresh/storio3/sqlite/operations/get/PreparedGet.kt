package com.pushtorefresh.storio3.sqlite.operations.get

import androidx.annotation.WorkerThread
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.impl.ChainImpl.buildChain
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.impl.ChangesFilter
import com.pushtorefresh.storio3.sqlite.queries.GetQuery
import com.pushtorefresh.storio3.sqlite.queries.Query
import com.pushtorefresh.storio3.sqlite.queries.RawQuery
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext

/**
 * Prepared Get Operation for [StorIOSQLite].
 *
 * @param <Result> type of result.
</Result> */
abstract class PreparedGet<Result> : PreparedOperation<Result, GetQuery> {

    protected val storIOSQLite: StorIOSQLite

    protected val query: Query?

    protected val rawQuery: RawQuery?

    internal constructor(storIOSQLite: StorIOSQLite, query: Query) {
        this.storIOSQLite = storIOSQLite
        this.query = query
        this.rawQuery = null
    }

    internal constructor(storIOSQLite: StorIOSQLite, rawQuery: RawQuery) {
        this.storIOSQLite = storIOSQLite
        this.rawQuery = rawQuery
        query = null
    }

    fun asFlow(): Flow<Result> {
        val tables = extractTables(query, rawQuery)
        val tags = extractTags(query, rawQuery)

        val f = if (tables.isNotEmpty() || tags.isNotEmpty()) {
            flow {
                coroutineScope {
                    emit(executeAsBlocking())
                    ChangesFilter.applyForTablesAndTags(storIOSQLite.observeChanges(), tables, tags)
                        .collect { emit(executeAsBlocking()) }
                }
            }
                .flowOn(storIOSQLite.defaultDispatcher())
        } else {
            flow { emit(executeAsBlocking()) }
        }

        return f as Flow<Result>
    }

    /**
     * Executes Get Operation immediately in current thread.
     *
     *
     * Notice: This is blocking I/O operation that should not be executed on the Main Thread,
     * it can cause ANR (Activity Not Responding dialog), block the UI and drop animations frames.
     * So please, call this method on some background thread. See [WorkerThread].
     *
     * @return result of an operation. Can be null in get(Object).
     */
    @WorkerThread
    override fun executeAsBlocking(): Result? {
        return buildChain(storIOSQLite.interceptors(), getRealCallInterceptor())
            .proceed(this)
    }

    override suspend fun await(): Result? {
        return withContext(storIOSQLite.defaultDispatcher()) {
            executeAsBlocking()
        }
    }

    override fun getData(): GetQuery {
        return rawQuery ?: (query ?: throw IllegalStateException("Either rawQuery or query should be set!"))
    }

    protected abstract fun getRealCallInterceptor(): Interceptor

    private fun extractTables(
        query: Query?,
        rawQuery: RawQuery?
    ): Set<String> {
        return if (query != null) {
            setOf(query.table())
        } else rawQuery?.observesTables() ?: throw IllegalStateException("Please specify query")
    }

    private fun extractTags(
        query: Query?,
        rawQuery: RawQuery?
    ): Set<String> {
        return query?.observesTags() ?: (rawQuery?.observesTags()
            ?: throw IllegalStateException("Please specify query"))
    }

    /**
     * Builder for [PreparedGet].
     */
    class Builder(private val storIOSQLite: StorIOSQLite) {

        /**
         * Returns builder for Get Operation that returns result as [android.database.Cursor].
         *
         * @return builder for Get Operation that returns result as [android.database.Cursor].
         */
        fun cursor(): PreparedGetCursor.Builder {
            return PreparedGetCursor.Builder(storIOSQLite)
        }

        /**
         * Returns builder for Get Operation that returns result as [java.util.List] of items.
         *
         * @param type type of items.
         * @param <T>  type of items.
         * @return builder for Get Operation that returns result as [java.util.List] of items.
        </T> */
        fun <T> listOfObjects(type: Class<T>): PreparedGetListOfObjects.Builder<T> {
            return PreparedGetListOfObjects.Builder(storIOSQLite, type)
        }

        /**
         * Returns builder for Get Operation that returns result as item instance.
         *
         * @param type type of item.
         * @param <T>  type of item.
         * @return builder for Get Operation that returns result as item instance.
        </T> */
        fun <T> `object`(type: Class<T>): PreparedGetObject.Builder<T> {
            return PreparedGetObject.Builder(storIOSQLite, type)
        }

        /**
         * Returns builder for Get Operation that returns number of results.
         *
         * @return builder for Get Operation that returns number of results.
         */
        fun numberOfResults(): PreparedGetNumberOfResults.Builder {
            return PreparedGetNumberOfResults.Builder(storIOSQLite)
        }
    }
}
