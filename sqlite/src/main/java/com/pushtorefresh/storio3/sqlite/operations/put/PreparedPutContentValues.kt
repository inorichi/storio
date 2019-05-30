package com.pushtorefresh.storio3.sqlite.operations.put

import android.content.ContentValues
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.StorIOSQLite

/**
 * Prepared Put Operation for [StorIOSQLite].
 */
class PreparedPutContentValues internal constructor(
    storIOSQLite: StorIOSQLite,
    private val contentValues: ContentValues,
    private val putResolver: PutResolver<ContentValues>
) : PreparedPut<PutResult, ContentValues>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    override fun getData(): ContentValues {
        return contentValues
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val putResult = putResolver.performPut(storIOSQLite, contentValues)
                if (putResult.wasInserted() || putResult.wasUpdated()) {
                    val changes = Changes.newInstance(putResult.affectedTables(), putResult.affectedTags())
                    storIOSQLite.lowLevel().notifyAboutChanges(changes)
                }

                return putResult as Result
            } catch (exception: Exception) {
                throw StorIOException(
                    "Error has occurred during Put operation. contentValues = $contentValues",
                    exception
                )
            }

        }
    }

    /**
     * Builder for [PreparedPutContentValues].
     */
    class Builder internal constructor(
        private val storIOSQLite: StorIOSQLite,
        private val contentValues: ContentValues
    ) {

        /**
         * Required: Specifies [PutResolver] for Put Operation
         * which allows you to customize behavior of Put Operation.
         *
         * @param putResolver put resolver.
         * @return builder.
         * @see DefaultPutResolver
         */
        fun withPutResolver(putResolver: PutResolver<ContentValues>): CompleteBuilder {
            return CompleteBuilder(
                storIOSQLite,
                contentValues,
                putResolver
            )
        }
    }

    /**
     * Compile-time safe part of [Builder].
     */
    class CompleteBuilder internal constructor(
        private val storIOSQLite: StorIOSQLite,
        private val contentValues: ContentValues,
        private val putResolver: PutResolver<ContentValues>
    ) {

        /**
         * Prepares Put Operation.
         *
         * @return [PreparedPutContentValues] instance.
         */
        fun prepare(): PreparedPutContentValues {
            return PreparedPutContentValues(
                storIOSQLite,
                contentValues,
                putResolver
            )
        }
    }
}
