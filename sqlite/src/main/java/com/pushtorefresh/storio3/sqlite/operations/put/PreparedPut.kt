package com.pushtorefresh.storio3.sqlite.operations.put

import android.content.ContentValues
import androidx.annotation.WorkerThread
import com.pushtorefresh.storio3.Interceptor
import com.pushtorefresh.storio3.impl.ChainImpl.buildChain
import com.pushtorefresh.storio3.operations.PreparedOperation
import com.pushtorefresh.storio3.sqlite.StorIOSQLite
import kotlinx.coroutines.withContext
import java.util.*

/**
 * Prepared Put Operation for [StorIOSQLite] which performs insert or update data
 * in [StorIOSQLite].
 */
abstract class PreparedPut<Result, Data> internal constructor(protected val storIOSQLite: StorIOSQLite) :
    PreparedOperation<Result, Data> {

    /**
     * Executes Put Operation immediately in current thread.
     *
     *
     * Notice: This is blocking I/O operation that should not be executed on the Main Thread,
     * it can cause ANR (Activity Not Responding dialog), block the UI and drop animations frames.
     * So please, call this method on some background thread. See [WorkerThread].
     *
     * @return non-null results of Put Operation.
     */
    @WorkerThread
    override fun executeAsBlocking(): Result {
        return buildChain(storIOSQLite.interceptors(), getRealCallInterceptor())
            .proceed<Result, Data>(this)
    }

    override suspend fun await(): Result {
        return withContext(storIOSQLite.defaultDispatcher()) {
            executeAsBlocking()
        }
    }

    protected abstract fun getRealCallInterceptor(): Interceptor

    /**
     * Builder for [PreparedPut].
     */
    class Builder(private val storIOSQLite: StorIOSQLite) {

        /**
         * Prepares Put Operation for one instance of [ContentValues].
         *
         * @param contentValues content values to put.
         * @return builder.
         */
        fun contentValues(contentValues: ContentValues): PreparedPutContentValues.Builder {
            return PreparedPutContentValues.Builder(storIOSQLite, contentValues)
        }

        /**
         * Prepares Put Operation for multiple [ContentValues].
         *
         * @param contentValuesIterable content values to put.
         * @return builder.
         */
        fun contentValues(contentValuesIterable: Iterable<ContentValues>): PreparedPutContentValuesIterable.Builder {
            return PreparedPutContentValuesIterable.Builder(storIOSQLite, contentValuesIterable)
        }

        /**
         * Prepares Put Operation for multiple [ContentValues].
         *
         * @param contentValuesArray content values to put.
         * @return builder.
         */
        fun contentValues(vararg contentValuesArray: ContentValues): PreparedPutContentValuesIterable.Builder {
            return PreparedPutContentValuesIterable.Builder(storIOSQLite, Arrays.asList(*contentValuesArray))
        }

        /**
         * Prepares Put Operation for one object.
         *
         * @param object object to put.
         * @param <T>    type of object.
         * @return builder.
        </T> */
        fun <T : Any> `object`(`object`: T): PreparedPutObject.Builder<T> {
            return PreparedPutObject.Builder(storIOSQLite, `object`)
        }

        /**
         * Prepares Put Operation for multiple objects.
         *
         * @param objects objects to put.
         * @param <T>     type of objects.
         * @return builder.
        </T> */
        fun <T : Any> objects(objects: Collection<T>): PreparedPutCollectionOfObjects.Builder<T> {
            return PreparedPutCollectionOfObjects.Builder(storIOSQLite, objects)
        }
    }
}
