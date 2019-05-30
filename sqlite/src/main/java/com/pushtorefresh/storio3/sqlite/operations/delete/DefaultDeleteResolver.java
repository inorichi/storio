package com.pushtorefresh.storio3.sqlite.operations.delete;

import androidx.annotation.NonNull;

import com.pushtorefresh.storio3.sqlite.StorIOSQLite;
import com.pushtorefresh.storio3.sqlite.queries.DeleteQuery;

/**
 * Default implementation of {@link DeleteResolver}.
 * Thread-safe.
 */
public abstract class DefaultDeleteResolver<T> extends DeleteResolver<T> {

    /**
     * Converts object to {@link DeleteQuery}.
     *
     * @param object object that should be deleted.
     * @return {@link DeleteQuery} that will be performed.
     */
    @NonNull
    protected abstract DeleteQuery mapToDeleteQuery(@NonNull T object);

    /**
     * {@inheritDoc}
     */
    @NonNull
    @Override
    public DeleteResult performDelete(@NonNull StorIOSQLite storIOSQLite, @NonNull T object) {
        final DeleteQuery deleteQuery = mapToDeleteQuery(object);
        final int numberOfRowsDeleted = storIOSQLite.lowLevel().delete(deleteQuery);
        return DeleteResult.newInstance(numberOfRowsDeleted, deleteQuery.table(), deleteQuery.affectsTags());
    }
}
