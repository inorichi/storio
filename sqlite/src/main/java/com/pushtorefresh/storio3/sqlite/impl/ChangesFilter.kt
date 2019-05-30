package com.pushtorefresh.storio3.sqlite.impl

import com.pushtorefresh.storio3.sqlite.Changes
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter

/**
 * FOR INTERNAL USAGE ONLY.
 *
 *
 * Hides RxJava from ClassLoader via separate class.
 */
class ChangesFilter private constructor(private val tables: Set<String>?, private val tags: Set<String>?) {

    @Throws(Exception::class)
    fun test(changes: Changes): Boolean {
        if (tables != null) {
            // if one of changed tables found in tables for subscription -> notify observer
            for (affectedTable in changes.affectedTables()) {
                if (tables.contains(affectedTable)) {
                    return true
                }
            }
        }
        if (tags != null) {
            // if one of changed tags found tag for subscription -> notify observer
            for (affectedTag in changes.affectedTags()) {
                if (tags.contains(affectedTag)) {
                    return true
                }
            }
        }

        return false
    }

    companion object {

        @JvmStatic
        fun applyForTables(changes: Flow<Changes>, tables: Set<String>): Flow<Changes> {
            val filter = ChangesFilter(tables, null)
            return changes.filter { filter.test(it) }
        }

        @JvmStatic
        fun applyForTags(changes: Flow<Changes>, tags: Set<String>): Flow<Changes> {
            val filter = ChangesFilter(null, tags)
            return changes.filter { filter.test(it) }
        }

        @JvmStatic
        fun applyForTablesAndTags(
            changes: Flow<Changes>,
            tables: Set<String>,
            tags: Set<String>
        ): Flow<Changes> {
            val filter = ChangesFilter(tables, tags)
            return changes.filter { filter.test(it) }
        }
    }
}
