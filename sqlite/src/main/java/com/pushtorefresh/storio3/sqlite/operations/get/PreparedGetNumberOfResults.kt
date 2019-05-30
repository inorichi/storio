package com.pushtorefresh.storio3.sqlite.operations.get

import android.database.Cursor
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.queries.Query
import com.pushtorefresh.storio3.sqlite.queries.RawQuery

class PreparedGetNumberOfResults : PreparedGetMandatoryResult<Int> {

    private val getResolver: GetResolver<Int>

    internal constructor(storIOSQLite: StorIOSQLite, query: Query, getResolver: GetResolver<Int>) : super(
        storIOSQLite,
        query
    ) {
        this.getResolver = getResolver
    }

    internal constructor(storIOSQLite: StorIOSQLite, rawQuery: RawQuery, getResolver: GetResolver<Int>) : super(
        storIOSQLite,
        rawQuery
    ) {
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
            val cursor: Cursor

            try {
                if (query != null) {
                    cursor = getResolver.performGet(storIOSQLite, query)
                } else if (rawQuery != null) {
                    cursor = getResolver.performGet(storIOSQLite, rawQuery)
                } else {
                    throw IllegalStateException("Please specify query")
                }

                try {

                    return getResolver.mapFromCursor(storIOSQLite, cursor) as Result
                } finally {
                    cursor.close()
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
     * Builder for [PreparedGetNumberOfResults].
     */
    class Builder internal constructor(private val storIOSQLite: StorIOSQLite) {

        /**
         * Required: Specifies query which will be passed to [StorIOSQLite]
         * to get list of objects.
         *
         * @param query non-null query.
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
     * Compile-time safe part of builder for [PreparedGetNumberOfResults].
     */
    class CompleteBuilder {

        private val storIOSQLite: StorIOSQLite

        internal var query: Query? = null

        internal var rawQuery: RawQuery? = null

        private var getResolver: GetResolver<Int>? = null

        internal constructor(storIOSQLite: StorIOSQLite, query: Query) {
            this.storIOSQLite = storIOSQLite
            this.query = query
            rawQuery = null
        }

        internal constructor(storIOSQLite: StorIOSQLite, rawQuery: RawQuery) {
            this.storIOSQLite = storIOSQLite
            this.rawQuery = rawQuery
            query = null
        }

        /**
         * Optional: Specifies resolver for Get Operation which can be used
         * to provide custom behavior of Get Operation.
         *
         *
         *
         * @param getResolver nullable resolver for Get Operation.
         * @return builder.
         */
        fun withGetResolver(getResolver: GetResolver<Int>?): CompleteBuilder {
            this.getResolver = getResolver
            return this
        }

        /**
         * Builds new instance of [PreparedGetNumberOfResults].
         *
         * @return new instance of [PreparedGetNumberOfResults].
         */
        fun prepare(): PreparedGetNumberOfResults {
            if (getResolver == null) {
                getResolver = STANDARD_GET_RESOLVER
            }

            return if (query != null) {
                PreparedGetNumberOfResults(
                    storIOSQLite,
                    query!!,
                    getResolver!!
                )
            } else if (rawQuery != null) {
                PreparedGetNumberOfResults(
                    storIOSQLite,
                    rawQuery!!,
                    getResolver!!
                )
            } else {
                throw IllegalStateException("Please specify query")
            }
        }

        companion object {

            internal val STANDARD_GET_RESOLVER: GetResolver<Int> = object : DefaultGetResolver<Int>() {
                override fun mapFromCursor(storIOSQLite: StorIOSQLite, cursor: Cursor): Int {
                    return cursor.count
                }
            }
        }
    }

}
