package com.pushtorefresh.storio3.sqlite.operations.get

import android.database.Cursor
import androidx.annotation.WorkerThread
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.SQLiteTypeMapping
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.queries.Query
import com.pushtorefresh.storio3.sqlite.queries.RawQuery

class PreparedGetObject<T> : PreparedGet<T> {

    private val type: Class<T>

    private val explicitGetResolver: GetResolver<T>?

    internal constructor(
        storIOSQLite: StorIOSQLite,
        type: Class<T>,
        query: Query,
        explicitGetResolver: GetResolver<T>?
    ) : super(storIOSQLite, query) {
        this.type = type
        this.explicitGetResolver = explicitGetResolver
    }

    internal constructor(
        storIOSQLite: StorIOSQLite,
        type: Class<T>,
        rawQuery: RawQuery,
        explicitGetResolver: GetResolver<T>?
    ) : super(storIOSQLite, rawQuery) {
        this.type = type
        this.explicitGetResolver = explicitGetResolver
    }

    /**
     * {@inheritDoc}
     */
    @WorkerThread
    override fun executeAsBlocking(): T? {
        return super.executeAsBlocking()
    }

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result? {
            try {
                val getResolver: GetResolver<T>

                if (explicitGetResolver != null) {
                    getResolver = explicitGetResolver
                } else {
                    val typeMapping = storIOSQLite.lowLevel().typeMapping(type) ?: throw IllegalStateException(
                        "This type does not have type mapping: " +
                                "type = " + type + "," +
                                "db was not touched by this operation, please add type mapping for this type"
                    )

                    getResolver = typeMapping.resolver
                }

                val cursor: Cursor

                if (query != null) {
                    cursor = getResolver.performGet(storIOSQLite, query)
                } else if (rawQuery != null) {
                    cursor = getResolver.performGet(storIOSQLite, rawQuery)
                } else {
                    throw IllegalStateException("Please specify query")
                }

                try {
                    val count = cursor.count

                    if (count == 0) {
                        return null
                    } else {
                        cursor.moveToNext()

                        return getResolver.mapFromCursor(storIOSQLite, cursor) as Result
                    }
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
     * Builder for [PreparedGetObject] Operation.
     *
     * @param <T> type of objects.
    </T> */
    class Builder<T> internal constructor(private val storIOSQLite: StorIOSQLite, private val type: Class<T>) {

        /**
         * Required: Specifies query which will be passed to [StorIOSQLite]
         * to get object.
         *
         * @param query non-null query.
         * @return builder.
         * @see Query
         */
        fun withQuery(query: Query): CompleteBuilder<T> {
            return CompleteBuilder(storIOSQLite, type, query)
        }

        /**
         * Required: Specifies [RawQuery] for Get Operation,
         * you can use it for "joins" and same constructions which are not allowed for [Query].
         *
         * @param rawQuery query.
         * @return builder.
         * @see RawQuery
         */
        fun withQuery(rawQuery: RawQuery): CompleteBuilder<T> {
            return CompleteBuilder(storIOSQLite, type, rawQuery)
        }
    }

    /**
     * Compile-safe part of [Builder].
     *
     * @param <T> type of objects.
    </T> */
    class CompleteBuilder<T> {

        private val storIOSQLite: StorIOSQLite

        private val type: Class<T>

        internal var query: Query? = null

        internal var rawQuery: RawQuery? = null

        private var getResolver: GetResolver<T>? = null

        internal constructor(storIOSQLite: StorIOSQLite, type: Class<T>, query: Query) {
            this.storIOSQLite = storIOSQLite
            this.type = type
            this.query = query
            rawQuery = null
        }

        internal constructor(storIOSQLite: StorIOSQLite, type: Class<T>, rawQuery: RawQuery) {
            this.storIOSQLite = storIOSQLite
            this.type = type
            this.rawQuery = rawQuery
            query = null
        }

        /**
         * Optional: Specifies resolver for Get Operation which can be used
         * to provide custom behavior of Get Operation.
         *
         *
         * [SQLiteTypeMapping] can be used to set default GetResolver.
         * If GetResolver is not set via [SQLiteTypeMapping]
         * or explicitly — exception will be thrown.
         *
         * @param getResolver nullable resolver for Get Operation.
         * @return builder.
         */
        fun withGetResolver(getResolver: GetResolver<T>?): CompleteBuilder<T> {
            this.getResolver = getResolver
            return this
        }

        /**
         * Builds new instance of [PreparedGetObject].
         *
         * @return new instance of [PreparedGetObject].
         */
        fun prepare(): PreparedGetObject<T> {
            return if (query != null) {
                PreparedGetObject(
                    storIOSQLite,
                    type,
                    query!!,
                    getResolver
                )
            } else if (rawQuery != null) {
                PreparedGetObject(
                    storIOSQLite,
                    type,
                    rawQuery!!,
                    getResolver
                )
            } else {
                throw IllegalStateException("Please specify Query or RawQuery")
            }
        }
    }
}
