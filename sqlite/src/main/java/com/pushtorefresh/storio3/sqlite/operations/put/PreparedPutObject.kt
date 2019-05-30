package com.pushtorefresh.storio3.sqlite.operations.put

import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.SQLiteTypeMapping
import com.pushtorefresh.storio3.sqlite.StorIOSQLite

/**
 * Prepared Put Operation for [StorIOSQLite].
 *
 * @param <T> type of object to put.
</T> */
class PreparedPutObject<T : Any> internal constructor(
    storIOSQLite: StorIOSQLite,
    private val obj: T,
    private val explicitPutResolver: PutResolver<T>?
) : PreparedPut<PutResult, T>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    override fun getData(): T {
        return obj
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val lowLevel = storIOSQLite.lowLevel()

                val putResolver: PutResolver<T>

                if (explicitPutResolver != null) {
                    putResolver = explicitPutResolver
                } else {
                    val typeMapping = lowLevel.typeMapping(getData().javaClass) ?: throw IllegalStateException(
                        "Object does not have type mapping: " +
                                "object = " + getData() + ", object.class = " + getData().javaClass + ", " +
                                "db was not affected by this operation, please add type mapping for this type"
                    )

                    putResolver = typeMapping.putResolver()
                }

                val putResult = putResolver.performPut(storIOSQLite, getData())

                if (putResult.wasInserted() || putResult.wasUpdated()) {
                    val changes = Changes.newInstance(
                        putResult.affectedTables(),
                        putResult.affectedTags()
                    )
                    lowLevel.notifyAboutChanges(changes)
                }


                return putResult as Result
            } catch (exception: Exception) {
                throw StorIOException("Error has occurred during Put operation. object = ${getData()}", exception)
            }

        }
    }

    /**
     * Builder for [PreparedPutObject].
     *
     * @param <T> type of object to put.
    </T> */
    class Builder<T : Any> internal constructor(private val storIOSQLite: StorIOSQLite, private val `object`: T) {

        private var putResolver: PutResolver<T>? = null

        /**
         * Optional: Specifies [PutResolver] for Put Operation
         * which allows you to customize behavior of Put Operation.
         *
         *
         * Can be set via [SQLiteTypeMapping]
         * If it's not set via [SQLiteTypeMapping] or explicitly — exception will be thrown.
         *
         * @param putResolver put resolver.
         * @return builder.
         * @see DefaultPutResolver
         */
        fun withPutResolver(putResolver: PutResolver<T>): Builder<T> {
            this.putResolver = putResolver
            return this
        }

        /**
         * Prepares Put Operation.
         *
         * @return [PreparedPutObject] instance.
         */
        fun prepare(): PreparedPutObject<T> {
            return PreparedPutObject(
                storIOSQLite,
                `object`,
                putResolver
            )
        }
    }
}
