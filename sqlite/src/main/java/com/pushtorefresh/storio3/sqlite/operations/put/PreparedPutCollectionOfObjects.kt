package com.pushtorefresh.storio3.sqlite.operations.put

import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.SQLiteTypeMapping
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import java.util.*
import java.util.AbstractMap.SimpleImmutableEntry

class PreparedPutCollectionOfObjects<T : Any> internal constructor(
    storIOSQLite: StorIOSQLite,
    private val objects: Collection<T>,
    private val explicitPutResolver: PutResolver<T>?,
    private val useTransaction: Boolean
) : PreparedPut<PutResults<T>, Collection<T>>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    override fun getData(): Collection<T> {
        return objects
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val lowLevel = storIOSQLite.lowLevel()

                // Nullable
                val objectsAndPutResolvers: MutableList<SimpleImmutableEntry<T, PutResolver<T>>>?

                if (explicitPutResolver != null) {
                    objectsAndPutResolvers = null
                } else {
                    objectsAndPutResolvers = ArrayList(objects.size)

                    for (`object` in objects) {

                        val typeMapping = lowLevel.typeMapping<Any>(`object`.javaClass) as SQLiteTypeMapping<T>?
                            ?: throw IllegalStateException(
                                "One of the objects from the collection does not have type mapping: " +
                                        "object = " + `object` + ", object.class = " + `object`.javaClass + "," +
                                        "db was not affected by this operation, please add type mapping for this type"
                            )

                        objectsAndPutResolvers.add(
                            SimpleImmutableEntry(
                                `object`,
                                typeMapping.putResolver()
                            )
                        )
                    }
                }

                if (useTransaction) {
                    lowLevel.beginTransaction()
                }

                val results = HashMap<T, PutResult>(objects.size)
                var transactionSuccessful = false

                try {
                    if (explicitPutResolver != null) {
                        for (`object` in objects) {
                            val putResult = explicitPutResolver.performPut(storIOSQLite, `object`)
                            results[`object`] = putResult

                            if (!useTransaction && (putResult.wasInserted() || putResult.wasUpdated())) {
                                val changes = Changes.newInstance(
                                    putResult.affectedTables(),
                                    putResult.affectedTags()
                                )
                                lowLevel.notifyAboutChanges(changes)
                            }
                        }
                    } else {
                        for ((obj, putResolver) in objectsAndPutResolvers!!) {

                            val putResult = putResolver.performPut(storIOSQLite, obj)

                            results[obj] = putResult

                            if (!useTransaction && (putResult.wasInserted() || putResult.wasUpdated())) {
                                val changes = Changes.newInstance(
                                    putResult.affectedTables(),
                                    putResult.affectedTags()
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

                        // if put was in transaction and it was successful -> notify about changes
                        if (transactionSuccessful) {
                            val affectedTables = HashSet<String>(1) // in most cases it will be 1 table
                            val affectedTags = HashSet<String>(1)

                            for (`object` in results.keys) {
                                val putResult = results[`object`]
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


                return PutResults.newInstance(results) as Result

            } catch (exception: Exception) {
                throw StorIOException("Error has occurred during Put operation. objects = $objects", exception)
            }

        }
    }

    /**
     * Builder for [PreparedPutCollectionOfObjects]
     *
     * @param <T> type of objects to put
    </T> */
    class Builder<T : Any> internal constructor(private val storIOSQLite: StorIOSQLite, private val objects: Collection<T>) {

        private var putResolver: PutResolver<T>? = null

        private var useTransaction = true

        /**
         * Optional: Specifies [PutResolver] for Put Operation
         * which allows you to customize behavior of Put Operation
         *
         *
         * Can be set via [SQLiteTypeMapping]
         * If it's not set via [SQLiteTypeMapping] or explicitly — exception will be thrown
         *
         * @param putResolver put resolver
         * @return builder
         * @see DefaultPutResolver
         */
        fun withPutResolver(putResolver: PutResolver<T>): Builder<T> {
            this.putResolver = putResolver
            return this
        }

        /**
         * Optional: Defines that Put Operation will use transaction if it is supported by implementation of [StorIOSQLite]
         *
         *
         * By default, transaction will be used
         *
         * @return builder
         */
        fun useTransaction(useTransaction: Boolean): Builder<T> {
            this.useTransaction = useTransaction
            return this
        }

        /**
         * Prepares Put Operation
         *
         * @return [PreparedPutCollectionOfObjects] instance
         */
        fun prepare(): PreparedPutCollectionOfObjects<T> {
            return PreparedPutCollectionOfObjects(
                storIOSQLite,
                objects,
                putResolver,
                useTransaction
            )
        }
    }
}
