package com.pushtorefresh.storio3.sqlite.operations.delete

import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.SQLiteTypeMapping
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import java.util.*
import java.util.AbstractMap.SimpleImmutableEntry

/**
 * Prepared Delete Operation for [StorIOSQLite].
 *
 * @param <T> type of objects to delete.
</T> */
class PreparedDeleteCollectionOfObjects<T : Any> internal constructor(
    storIOSQLite: StorIOSQLite,
    private val objects: Collection<T>,
    private val explicitDeleteResolver: DeleteResolver<T>?,
    private val useTransaction: Boolean
) : PreparedDelete<DeleteResults<T>, Collection<T>>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val lowLevel = storIOSQLite.lowLevel()

                // Nullable
                val objectsAndDeleteResolvers: MutableList<SimpleImmutableEntry<T, DeleteResolver<T>>>?

                if (explicitDeleteResolver != null) {
                    objectsAndDeleteResolvers = null
                } else {
                    objectsAndDeleteResolvers = ArrayList(objects.size)

                    for (`object` in objects) {

                        val typeMapping = lowLevel.typeMapping<Any>(`object`.javaClass) as SQLiteTypeMapping<T>?
                            ?: throw IllegalStateException(
                                "One of the objects from the collection does not have type mapping: " +
                                        "object = " + `object` + ", object.class = " + `object`.javaClass + "," +
                                        "db was not affected by this operation, please add type mapping for this type"
                            )

                        objectsAndDeleteResolvers.add(
                            SimpleImmutableEntry(
                                `object`,
                                typeMapping.deleteResolver()
                            )
                        )
                    }
                }

                if (useTransaction) {
                    lowLevel.beginTransaction()
                }

                val results = HashMap<T, DeleteResult>(objects.size)
                var transactionSuccessful = false

                try {
                    if (explicitDeleteResolver != null) {
                        for (`object` in objects) {
                            val deleteResult = explicitDeleteResolver.performDelete(storIOSQLite, `object`)

                            results[`object`] = deleteResult

                            if (!useTransaction && deleteResult.numberOfRowsDeleted() > 0) {
                                val changes = Changes.newInstance(
                                    deleteResult.affectedTables(),
                                    deleteResult.affectedTags()
                                )
                                lowLevel.notifyAboutChanges(changes)
                            }
                        }
                    } else {
                        for ((obj, deleteResolver) in objectsAndDeleteResolvers!!) {

                            val deleteResult = deleteResolver.performDelete(storIOSQLite, obj)

                            results[obj] = deleteResult

                            if (!useTransaction && deleteResult.numberOfRowsDeleted() > 0) {
                                val changes = Changes.newInstance(
                                    deleteResult.affectedTables(),
                                    deleteResult.affectedTags()
                                )
                                lowLevel.notifyAboutChanges(changes)
                            }
                        }
                    }

                    if (useTransaction) {
                        lowLevel.setTransactionSuccessful()
                        transactionSuccessful = true
                    }
                } finally {
                    if (useTransaction) {
                        lowLevel.endTransaction()

                        // if delete was in transaction and it was successful -> notify about changes
                        if (transactionSuccessful) {
                            val affectedTables = HashSet<String>(1) // in most cases it will be one table
                            val affectedTags = HashSet<String>(1)

                            for (`object` in results.keys) {
                                val deleteResult = results[`object`]
                                if (deleteResult!!.numberOfRowsDeleted() > 0) {
                                    affectedTables.addAll(results[`object`]!!.affectedTables())
                                    affectedTags.addAll(results[`object`]!!.affectedTags())
                                }
                            }

                            // IMPORTANT: Notifying about change should be done after end of transaction
                            // It'll reduce number of possible deadlock situations
                            if (!affectedTables.isEmpty() || !affectedTags.isEmpty()) {
                                val changes = Changes.newInstance(affectedTables, affectedTags)
                                lowLevel.notifyAboutChanges(changes)
                            }
                        }
                    }
                }


                return DeleteResults.newInstance(results) as Result

            } catch (exception: Exception) {
                throw StorIOException("Error has occurred during Delete operation. objects = $objects", exception)
            }

        }
    }

    override fun getData(): Collection<T> {
        return objects
    }

    /**
     * Builder for [PreparedDeleteCollectionOfObjects].
     *
     * @param <T> type of objects to delete.
    </T> */
    class Builder<T : Any> internal constructor(private val storIOSQLite: StorIOSQLite, private val objects: Collection<T>) {

        private var deleteResolver: DeleteResolver<T>? = null

        private var useTransaction = true

        /**
         * Optional: Defines that Delete Operation will use transaction or not.
         *
         *
         * By default, transaction will be used.
         *
         * @param useTransaction `true` to use transaction, `false` to not.
         * @return builder.
         */
        fun useTransaction(useTransaction: Boolean): Builder<T> {
            this.useTransaction = useTransaction
            return this
        }

        /**
         * Optional: Specifies [DeleteResolver] for Delete Operation.
         *
         *
         *
         *
         * Can be set via [SQLiteTypeMapping],
         * If value is not set via [SQLiteTypeMapping]
         * or explicitly — exception will be thrown.
         *
         * @param deleteResolver [DeleteResolver] for Delete Operation.
         * @return builder.
         */
        fun withDeleteResolver(deleteResolver: DeleteResolver<T>?): Builder<T> {
            this.deleteResolver = deleteResolver
            return this
        }

        /**
         * Prepares Delete Operation.
         *
         * @return [PreparedDeleteCollectionOfObjects].
         */
        fun prepare(): PreparedDeleteCollectionOfObjects<T> {
            return PreparedDeleteCollectionOfObjects(
                storIOSQLite,
                objects,
                deleteResolver,
                useTransaction
            )
        }
    }
}
