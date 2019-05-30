package com.pushtorefresh.storio3.sqlite.operations.put

import android.content.ContentValues
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import java.util.*

class PreparedPutContentValuesIterable internal constructor(
    storIOSQLite: StorIOSQLite,
    private val contentValuesIterable: Iterable<ContentValues>,
    private val putResolver: PutResolver<ContentValues>,
    private val useTransaction: Boolean
) : PreparedPut<PutResults<ContentValues>, Iterable<ContentValues>>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    override fun getData(): Iterable<ContentValues> {
        return contentValuesIterable
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val lowLevel = storIOSQLite.lowLevel()

                val putResults = HashMap<ContentValues, PutResult>()

                if (useTransaction) {
                    lowLevel.beginTransaction()
                }

                var transactionSuccessful = false

                try {
                    for (contentValues in contentValuesIterable) {
                        val putResult = putResolver.performPut(storIOSQLite, contentValues)
                        putResults[contentValues] = putResult

                        if (!useTransaction && (putResult.wasInserted() || putResult.wasUpdated())) {
                            val changes = Changes.newInstance(
                                putResult.affectedTables(),
                                putResult.affectedTags()
                            )
                            lowLevel.notifyAboutChanges(changes)
                        }
                    }

                    if (useTransaction) {
                        lowLevel.setTransactionSuccessful()
                        transactionSuccessful = true
                    }
                } finally {
                    if (useTransaction) {
                        lowLevel.endTransaction()

                        if (transactionSuccessful) {
                            val affectedTables = HashSet<String>(1) // in most cases it will be 1 table
                            val affectedTags = HashSet<String>(1)

                            for (contentValues in putResults.keys) {
                                val putResult = putResults[contentValues]
                                if (putResult!!.wasInserted() || putResult.wasUpdated()) {
                                    affectedTables.addAll(putResult.affectedTables())
                                    affectedTags.addAll(putResult.affectedTags())
                                }
                            }

                            // IMPORTANT: Notifying about change should be done after end of transaction
                            // It'll reduce number of possible deadlock situations
                            if (!affectedTables.isEmpty() || !affectedTags.isEmpty()) {
                                lowLevel.notifyAboutChanges(Changes.newInstance(affectedTables, affectedTags))
                            }
                        }
                    }
                }

                return PutResults.newInstance(putResults) as Result

            } catch (exception: Exception) {
                throw StorIOException(
                    "Error has occurred during Put operation. contentValues = $contentValuesIterable",
                    exception
                )
            }

        }
    }

    /**
     * Builder for [PreparedPutContentValuesIterable]
     */
    class Builder internal constructor(
        private val storIOSQLite: StorIOSQLite,
        private val contentValuesIterable: Iterable<ContentValues>
    ) {

        /**
         * Required: Specifies [PutResolver] for Put Operation
         * which allows you to customize behavior of Put Operation
         *
         * @param putResolver put resolver
         * @return builder
         * @see DefaultPutResolver
         */
        fun withPutResolver(putResolver: PutResolver<ContentValues>): CompleteBuilder {
            return CompleteBuilder(
                storIOSQLite,
                contentValuesIterable,
                putResolver
            )
        }
    }

    /**
     * Compile-time safe part of [Builder]
     */
    class CompleteBuilder internal constructor(
        private val storIOSQLite: StorIOSQLite,
        private val contentValuesIterable: Iterable<ContentValues>,
        private val putResolver: PutResolver<ContentValues>
    ) {

        private var useTransaction = true

        /**
         * Optional: Defines that Put Operation will use transaction
         * if it is supported by implementation of [StorIOSQLite]
         *
         *
         * By default, transaction will be used
         *
         * @return builder
         */
        fun useTransaction(useTransaction: Boolean): CompleteBuilder {
            this.useTransaction = useTransaction
            return this
        }

        /**
         * Prepares Put Operation
         *
         * @return [PreparedPutContentValuesIterable] instance
         */
        fun prepare(): PreparedPutContentValuesIterable {
            return PreparedPutContentValuesIterable(
                storIOSQLite,
                contentValuesIterable,
                putResolver,
                useTransaction
            )
        }
    }
}
