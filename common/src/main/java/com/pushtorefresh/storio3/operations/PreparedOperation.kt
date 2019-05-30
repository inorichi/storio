package com.pushtorefresh.storio3.operations

import androidx.annotation.WorkerThread

/**
 * Common API of all prepared operations
 *
 * @param <Result>        type of result
 * @param <Data>          object that describes this operation inside interceptor for example
 */
interface PreparedOperation<Result, Data> {

    /**
     * Returns the data of this operation.
     */
    fun getData(): Data

    /**
     * Executes operation synchronously in current thread.
     *
     * Notice: Blocking I/O operation should not be executed on the Main Thread,
     * it can cause ANR (Activity Not Responding dialog), block the UI and drop animations frames.
     * So please, execute blocking I/O operation only from background thread.
     * See [WorkerThread].
     *
     * @return nullable result of operation.
     */
    @WorkerThread
    fun executeAsBlocking(): Result?

    /**
     * Executes operation suspending the current thread if necessary.
     */
    suspend fun await(): Result?

}
