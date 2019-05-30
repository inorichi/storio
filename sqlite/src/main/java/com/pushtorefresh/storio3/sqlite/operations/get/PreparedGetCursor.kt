package com.pushtorefresh.storio3.sqlite.operations.get

import android.database.Cursor
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.queries.Query
import com.pushtorefresh.storio3.sqlite.queries.RawQuery

/**
 * Prepared Get Operation for [StorIOSQLite].
 */
class PreparedGetCursor : PreparedGetMandatoryResult<Cursor> {

    private val getResolver: GetResolver<Cursor>

    internal constructor(
        storIOSQLite: StorIOSQLite,
        query: Query,
        getResolver: GetResolver<Cursor>
    ) : super(storIOSQLite, query) {
        this.getResolver = getResolver
    }

    internal constructor(
        storIOSQLite: StorIOSQLite,
        rawQuery: RawQuery,
        getResolver: GetResolver<Cursor>
    ) : super(storIOSQLite, rawQuery) {
        this.getResolver = getResolver
    }

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                return if (query != null) {

                    getResolver.performGet(storIOSQLite, query) as Result
                } else if (rawQuery != null) {

                    getResolver.performGet(storIOSQLite, rawQuery) as Result
                } else {
                    throw IllegalStateException("Please specify query")
                }
            } catch (exception: Exception) {
                throw StorIOException(
                    "Error has occurred during Get operation. query = " + (query ?: rawQuery),
                    exception
                )
            }

        }
    }

    /**
     * Builder for [PreparedGetCursor].
     *
     *
     * Required: You should specify query by call
     * [.withQuery] or [.withQuery].
     */
    class Builder internal constructor(private val storIOSQLite: StorIOSQLite) {

        /**
         * Required: Specifies [Query] for Get Operation.
         *
         * @param query query.
         * @return builder.
         * @see Query
         */
        fun withQuery(query: Query): CompleteBuilder {
            return CompleteBuilder(storIOSQLite, query)
        }

        /**
         * Required: Specifies [RawQuery] for Get Operation,
         * you can use it for "joins" and same constructions which are not allowed for [Query].
         *
         * @param rawQuery query.
         * @return builder.
         * @see RawQuery
         */
        fun withQuery(rawQuery: RawQuery): CompleteBuilder {
            return CompleteBuilder(storIOSQLite, rawQuery)
        }
    }

    /**
     * Compile-time safe part of builder for [PreparedGetCursor].
     */
    class CompleteBuilder {

        private val storIOSQLite: StorIOSQLite

        internal var query: Query? = null

        internal var rawQuery: RawQuery? = null

        private var getResolver: GetResolver<Cursor>? = null

        internal constructor(storIOSQLite: StorIOSQLite, query: Query) {
            this.storIOSQLite = storIOSQLite
            this.query = query
            this.rawQuery = null
        }

        internal constructor(storIOSQLite: StorIOSQLite, rawQuery: RawQuery) {
            this.storIOSQLite = storIOSQLite
            this.rawQuery = rawQuery
            this.query = null
        }

        /**
         * Optional: Specifies Get Resolver for operation.
         * If no value is set, builder will use resolver that
         * simply redirects query to [StorIOSQLite].
         *
         * @param getResolver nullable GetResolver for Get Operation.
         * @return builder.
         */
        fun withGetResolver(getResolver: GetResolver<Cursor>?): CompleteBuilder {
            this.getResolver = getResolver
            return this
        }

        /**
         * Prepares Get Operation.
         *
         * @return [PreparedGetCursor] instance.
         */
        fun prepare(): PreparedGetCursor {
            if (getResolver == null) {
                getResolver = STANDARD_GET_RESOLVER
            }

            return if (query != null) {
                PreparedGetCursor(storIOSQLite, query!!, getResolver!!)
            } else if (rawQuery != null) {
                PreparedGetCursor(storIOSQLite, rawQuery!!, getResolver!!)
            } else {
                throw IllegalStateException("Please specify query")
            }
        }

        companion object {

            internal val STANDARD_GET_RESOLVER: GetResolver<Cursor> = object : DefaultGetResolver<Cursor>() {
                override fun mapFromCursor(storIOSQLite: StorIOSQLite, cursor: Cursor): Cursor {
                    return cursor // no modifications
                }
            }
        }
    }
}
