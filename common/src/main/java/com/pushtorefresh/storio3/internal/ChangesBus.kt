package com.pushtorefresh.storio3.internal

import kotlinx.coroutines.channels.BroadcastChannel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow

/**
 * FOR INTERNAL USE ONLY.
 *
 *
 * Thread-safe changes bus.
 */
class ChangesBus<T> {

    private val bus = BroadcastChannel<T>(1)

    fun onNext(next: T) {
        bus.offer(next)
    }

    fun asFlow(): Flow<T> {
        return bus.asFlow()
    }
}
