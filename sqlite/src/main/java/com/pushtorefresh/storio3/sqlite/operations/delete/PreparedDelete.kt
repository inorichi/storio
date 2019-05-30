package com.pushtorefresh.storio3.sqlite.operations.delete

import androidx.annotation.WorkerThread
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.impl.ChainImpl.buildChain
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import com.pushtorefresh.storio3.sqlite.queries.DeleteQuery
import kotlinx.coroutines.withContext

/**
 * Prepared Delete Operation for [StorIOSQLite].
 *
 * @param <T> type of object to delete.
</T> */
abstract class PreparedDelete<T, Data> internal constructor(protected val storIOSQLite: StorIOSQLite) :
    PreparedOperation<T, Data> {

    /**
     * Executes Delete Operation immediately in current thread.
     *
     *
     * Notice: This is blocking I/O operation that should not be executed on the Main Thread,
     * it can cause ANR (Activity Not Responding dialog), block the UI and drop animations frames.
     * So please, call this method on some background thread. See [WorkerThread].
     *
     * @return non-null result of Delete Operation.
     */
    @WorkerThread
    override fun executeAsBlocking(): T {
        return buildChain(storIOSQLite.interceptors(), getRealCallInterceptor())
            .proceed(this)
    }

    override suspend fun await(): T {
        return withContext(storIOSQLite.defaultDispatcher()) {
            executeAsBlocking()
        }
    }

    protected abstract fun getRealCallInterceptor(): Interceptor

    /**
     * Builder for [PreparedDelete].
     */
    class Builder(private val storIOSQLite: StorIOSQLite) {

        /**
         * Prepares Delete Operation by [com.pushtorefresh.storio3.sqlite.queries.DeleteQuery].
         *
         * @param deleteQuery query that specifies which rows should be deleted.
         * @return builder.
         */
        fun byQuery(deleteQuery: DeleteQuery): PreparedDeleteByQuery.Builder {
            return PreparedDeleteByQuery.Builder(storIOSQLite, deleteQuery)
        }

        /**
         * Prepares Delete Operation which should delete one object.
         *
         * @param object object to delete.
         * @param <T>    type of the object.
         * @return builder.
        </T> */
        fun <T : Any> `object`(`object`: T): PreparedDeleteObject.Builder<T> {
            return PreparedDeleteObject.Builder<T>(storIOSQLite, `object`)
        }

        /**
         * Prepares Delete Operation which should delete multiple objects.
         *
         * @param objects objects to delete.
         * @param <T>     type of objects.
         * @return builder.
        </T> */
        fun <T : Any> objects(objects: Collection<T>): PreparedDeleteCollectionOfObjects.Builder<T> {
            return PreparedDeleteCollectionOfObjects.Builder<T>(storIOSQLite, objects)
        }
    }
}
