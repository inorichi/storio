package com.pushtorefresh.storio3.sqlite.operations.delete

import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.SQLiteTypeMapping
import com.pushtorefresh.storio3.sqlite.StorIOSQLite

/**
 * Prepared Delete Operation for [StorIOSQLite].
 *
 * @param <T> type of object to delete.
</T> */
class PreparedDeleteObject<T : Any> internal constructor(
    storIOSQLite: StorIOSQLite,
    private val data: T,
    private val explicitDeleteResolver: DeleteResolver<T>?
) : PreparedDelete<DeleteResult, T>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    override fun getData(): T {
        return data
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val lowLevel = storIOSQLite.lowLevel()

                val deleteResolver: DeleteResolver<T>

                if (explicitDeleteResolver != null) {
                    deleteResolver = explicitDeleteResolver
                } else {
                    val typeMapping =
                        lowLevel.typeMapping(data.javaClass) ?: throw IllegalStateException(
                            "Object does not have type mapping: " +
                                    "object = " + data + ", object.class = " + data.javaClass + ", " +
                                    "db was not affected by this operation, please add type mapping for this type"
                        )

                    deleteResolver = typeMapping.deleteResolver()
                }

                val deleteResult = deleteResolver.performDelete(storIOSQLite, data)
                if (deleteResult.numberOfRowsDeleted() > 0) {
                    val changes = Changes.newInstance(
                        deleteResult.affectedTables(),
                        deleteResult.affectedTags()
                    )
                    lowLevel.notifyAboutChanges(changes)
                }
                return deleteResult as Result

            } catch (exception: Exception) {
                throw StorIOException("Error has occurred during Delete operation. object = $data", exception)
            }

        }

    }

    /**
     * Builder for [PreparedDeleteObject].
     *
     * @param <T> type of object to delete.
    </T> */
    class Builder<T : Any> internal constructor(private val storIOSQLite: StorIOSQLite, private val `object`: T) {

        private var deleteResolver: DeleteResolver<T>? = null

        /**
         * Optional: Specifies [DeleteResolver] for Delete Operation.
         *
         *
         * Can be set via [SQLiteTypeMapping],
         * If resolver is not set via [SQLiteTypeMapping]
         * or explicitly — exception will be thrown.
         *
         * @param deleteResolver delete resolver.
         * @return builder.
         */
        fun withDeleteResolver(deleteResolver: DeleteResolver<T>): Builder<T> {
            this.deleteResolver = deleteResolver
            return this
        }

        /**
         * Prepares Delete Operation.
         *
         * @return [PreparedDeleteObject] instance.
         */
        fun prepare(): PreparedDeleteObject<T> {
            return PreparedDeleteObject(
                storIOSQLite,
                `object`,
                deleteResolver
            )
        }
    }
}
