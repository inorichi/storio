package com.pushtorefresh.storio3.sqlite.operations.delete

import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.StorIOException
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.Changes
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.queries.DeleteQuery

/**
 * Prepared Delete Operation for [StorIOSQLite].
 */
class PreparedDeleteByQuery internal constructor(
    storIOSQLite: StorIOSQLite,
    private val deleteQuery: DeleteQuery,
    private val deleteResolver: DeleteResolver<DeleteQuery>
) : PreparedDelete<DeleteResult, DeleteQuery>(storIOSQLite) {

    override fun getRealCallInterceptor(): Interceptor {
        return RealCallInterceptor()
    }

    override fun getData(): DeleteQuery {
        return deleteQuery
    }

    private inner class RealCallInterceptor : Interceptor {
        override fun <Result, Data> intercept(
            operation: PreparedOperation<Result, Data>,
            chain: Interceptor.Chain
        ): Result {
            try {
                val deleteResult = deleteResolver.performDelete(storIOSQLite, deleteQuery)
                if (deleteResult.numberOfRowsDeleted() > 0) {
                    val changes = Changes.newInstance(
                        deleteResult.affectedTables(),
                        deleteResult.affectedTags()
                    )
                    storIOSQLite.lowLevel().notifyAboutChanges(changes)
                }

                return deleteResult as Result
            } catch (exception: Exception) {
                throw StorIOException("Error has occurred during Delete operation. query = $deleteQuery", exception)
            }

        }
    }

    /**
     * Builder for [PreparedDeleteByQuery].
     */
    class Builder internal constructor(private val storIOSQLite: StorIOSQLite, private val deleteQuery: DeleteQuery) {

        private var deleteResolver: DeleteResolver<DeleteQuery>? = null

        /**
         * Optional: Specifies Delete Resolver for Delete Operation.
         *
         *
         * If no value was specified, builder will use resolver that
         * simply redirects query to [StorIOSQLite].
         *
         * @param deleteResolver nullable resolver for Delete Operation.
         * @return builder.
         */
        fun withDeleteResolver(deleteResolver: DeleteResolver<DeleteQuery>?): Builder {
            this.deleteResolver = deleteResolver
            return this
        }

        /**
         * Prepares Delete Operation.
         *
         * @return [PreparedDeleteByQuery] instance.
         */
        fun prepare(): PreparedDeleteByQuery {
            if (deleteResolver == null) {
                deleteResolver = STANDARD_DELETE_RESOLVER
            }

            return PreparedDeleteByQuery(storIOSQLite, deleteQuery, deleteResolver!!)
        }

        companion object {

            private val STANDARD_DELETE_RESOLVER = object : DefaultDeleteResolver<DeleteQuery>() {
                public override fun mapToDeleteQuery(deleteQuery: DeleteQuery): DeleteQuery {
                    return deleteQuery // no transformations
                }
            }
        }
    }
}
