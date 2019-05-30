package com.pushtorefresh.storio3.sqlite;

import android.content.ContentValues;
import android.database.Cursor;
import androidx.annotation.NonNull;
import androidx.annotation.Nullable;
import androidx.annotation.WorkerThread;
import androidx.sqlite.db.SupportSQLiteOpenHelper;
import com.pushtorefresh.storio3.Interceptor;
import com.pushtorefresh.storio3.sqlite.operations.delete.PreparedDelete;
import com.pushtorefresh.storio3.sqlite.operations.execute.PreparedExecuteSQL;
import com.pushtorefresh.storio3.sqlite.operations.get.PreparedGet;
import com.pushtorefresh.storio3.sqlite.operations.put.PreparedPut;
import com.pushtorefresh.storio3.sqlite.queries.*;
import kotlinx.coroutines.CoroutineDispatcher;
import kotlinx.coroutines.flow.Flow;

import java.io.Closeable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.pushtorefresh.storio3.internal.Checks.checkNotEmpty;

/**
 * Powerful but simple abstraction for {@link android.database.sqlite.SQLiteDatabase}.
 * <p/>
 * It's an abstract class instead of interface because we want to have ability to add some
 * changes without breaking existing implementations.
 */
public abstract class StorIOSQLite implements Closeable {

    /**
     * Prepares "Execute SQL" Operation for {@link StorIOSQLite}.
     * Allows to execute a single SQL statement that is NOT a SELECT/INSERT/UPDATE/DELETE.
     *
     * @return builder for {@link PreparedExecuteSQL}.
     */
    @NonNull
    public PreparedExecuteSQL.Builder executeSQL() {
        return new PreparedExecuteSQL.Builder(this);
    }

    /**
     * Prepares "Get" Operation for {@link StorIOSQLite}.
     * Allows to get information from {@link StorIOSQLite}.
     *
     * @return builder for {@link PreparedGet}.
     */
    @NonNull
    public PreparedGet.Builder get() {
        return new PreparedGet.Builder(this);
    }

    /**
     * Prepares "Put" Operation for {@link StorIOSQLite}.
     * Allows to insert/update information in {@link StorIOSQLite}.
     *
     * @return builder for {@link PreparedPut}.
     */
    @NonNull
    public PreparedPut.Builder put() {
        return new PreparedPut.Builder(this);
    }

    /**
     * Prepares "Delete" Operation for {@link StorIOSQLite}.
     * Allows to delete information from {@link StorIOSQLite}.
     *
     * @return builder for {@link PreparedDelete}.
     */
    @NonNull
    public PreparedDelete.Builder delete() {
        return new PreparedDelete.Builder(this);
    }

    @NonNull
    public abstract Flow<Changes> observeChanges();

    @NonNull
    public abstract Flow<Changes> observeChangesInTables(@NonNull Set<String> tables);

    @NonNull
    public Flow<Changes> observeChangesInTable(@NonNull String table) {
        checkNotEmpty(table, "Table can not be null or empty");
        return observeChangesInTables(Collections.singleton(table));
    }

    @NonNull
    public abstract Flow<Changes> observeChangesOfTags(@NonNull Set<String> tags);

    @NonNull
    public Flow<Changes> observeChangesOfTag(@NonNull String tag) {
        checkNotEmpty(tag, "Tag can not be null or empty");
        return observeChangesOfTags(Collections.singleton(tag));
    }

    @NonNull
    public abstract CoroutineDispatcher defaultDispatcher();

    /**
     * An API for low level interaction with DB, it's part of public API, so feel free to use it,
     * but please read documentation carefully!
     *
     * @return implementation of low level APIs for {@link StorIOSQLite}.
     */
    @NonNull
    public abstract LowLevel lowLevel();

    /**
     * Provides list of added interceptors.
     *
     * @return unmodified list of added {@link Interceptor} (can be empty).
     */
    @NonNull
    public abstract List<Interceptor> interceptors();

    /**
     * API for low level operations with {@link StorIOSQLite}, we made it separate
     * to make {@link StorIOSQLite} API clean and easy to understand.
     */
    public static abstract class LowLevel {

        /**
         * Gets {@link SQLiteTypeMapping} for required type.
         * <p/>
         * Result can be {@code null}.
         *
         * @param type type.
         * @param <T>  type.
         * @return {@link SQLiteTypeMapping} for required type or {@code null}.
         */
        @Nullable
        public abstract <T> SQLiteTypeMapping<T> typeMapping(@NonNull Class<T> type);

        /**
         * Executes a single SQL statement that
         * is NOT a SELECT/INSERT/UPDATE/DELETE on the database.
         *
         * @param rawQuery sql query.
         */
        @WorkerThread
        public abstract void executeSQL(@NonNull RawQuery rawQuery);

        /**
         * Executes raw query on the database
         * and returns {@link android.database.Cursor} over the result set.
         *
         * @param rawQuery sql query
         * @return A Cursor object, which is positioned before the first entry.
         * Note that Cursors are not synchronized, see the documentation for more details.
         */
        @WorkerThread
        @NonNull
        public abstract Cursor rawQuery(@NonNull RawQuery rawQuery);

        /**
         * Executes query on the database and returns {@link android.database.Cursor}
         * over the result set.
         *
         * @param query sql query.
         * @return A Cursor object, which is positioned before the first entry.
         * Note that Cursors are not synchronized, see the documentation for more details.
         */
        @WorkerThread
        @NonNull
        public abstract Cursor query(@NonNull Query query);

        /**
         * Inserts a row into the database.
         *
         * @param insertQuery   query.
         * @param contentValues map that contains the initial column values for the row.
         *                      The keys should be the column names and the values the column values.
         * @return id of inserted row.
         */
        @WorkerThread
        public abstract long insert(@NonNull InsertQuery insertQuery, @NonNull ContentValues contentValues);

        /**
         * Inserts a row into the database.
         *
         * @param insertQuery       query.
         * @param contentValues     map that contains the initial column values for the row.
         *                          The keys should be the column names and the values the column values.
         * @param conflictAlgorithm for insert conflict resolver.
         * @return the row ID of the newly inserted row OR the primary key of the existing row
         * if the input param 'conflictAlgorithm' = {@link android.database.sqlite.SQLiteDatabase#CONFLICT_IGNORE}
         * OR -1 if any error.
         * @see android.database.sqlite.SQLiteDatabase#insertWithOnConflict(String, String, ContentValues, int)
         * @see android.database.sqlite.SQLiteDatabase#CONFLICT_REPLACE
         * @see android.database.sqlite.SQLiteDatabase#CONFLICT_ABORT
         * @see android.database.sqlite.SQLiteDatabase#CONFLICT_FAIL
         * @see android.database.sqlite.SQLiteDatabase#CONFLICT_ROLLBACK
         * @see android.database.sqlite.SQLiteDatabase#CONFLICT_IGNORE
         */
        @WorkerThread
        public abstract long insertWithOnConflict(@NonNull InsertQuery insertQuery, @NonNull ContentValues contentValues, int conflictAlgorithm);

        /**
         * Updates one or multiple rows in the database.
         *
         * @param updateQuery   query.
         * @param contentValues a map from column names to new column values.
         *                      {@code null} is a valid value that will be translated to {@code NULL}.
         * @return the number of rows affected.
         */
        @WorkerThread
        public abstract int update(@NonNull UpdateQuery updateQuery, @NonNull ContentValues contentValues);

        /**
         * Deletes one or multiple rows in the database.
         *
         * @param deleteQuery query.
         * @return the number of rows deleted.
         */
        @WorkerThread
        public abstract int delete(@NonNull DeleteQuery deleteQuery);

        /**
         * Notifies subscribers about changes happened in {@link StorIOSQLite}.
         * Operations can be executed in transaction or one operation can affect multiple tables,
         * so to reduce number of notifications you can call this method once and
         * provide aggregated Changes object.
         *
         * @param changes changes happened in {@link StorIOSQLite}.
         */
        public abstract void notifyAboutChanges(@NonNull Changes changes);

        /**
         * Begins a transaction in EXCLUSIVE mode.
         * <p>
         * Thread will be blocked on call to this method if another thread already in transaction,
         * as soon as first thread will end its transaction this thread will be unblocked.
         * <p>
         * Transactions can be nested.
         * When the outer transaction is ended all of
         * the work done in that transaction and all of the nested transactions will be committed or
         * rolled back. The changes will be rolled back if any transaction is ended without being
         * marked as clean (by calling setTransactionSuccessful). Otherwise they will be committed.
         * </p>
         * <p>Here is the standard idiom for transactions:
         * <p/>
         * <pre>
         *   db.beginTransaction();
         *   try {
         *     ...
         *     db.setTransactionSuccessful();
         *   } finally {
         *     db.endTransaction();
         *   }
         * </pre>
         */
        public abstract void beginTransaction();

        /**
         * Marks the current transaction as successful. Do not do any more database work between
         * calling this and calling endTransaction. Do as little non-database work as possible in that
         * situation too. If any errors are encountered between this and endTransaction the transaction
         * will still be committed.
         *
         * @throws IllegalStateException if the transaction is already marked as successful.
         */
        public abstract void setTransactionSuccessful();

        /**
         * Ends a transaction. See {@link #beginTransaction()} for notes about
         * how to use this and when transactions are committed and rolled back.
         */
        public abstract void endTransaction();

        /**
         * Returns {@link SupportSQLiteOpenHelper} for the direct access to underlining database.
         * It can be used in cases when {@link StorIOSQLite} APIs are not enough.
         * <p>
         * Notice: Database changes through the direct access
         * to the {@link SupportSQLiteOpenHelper} will not trigger notifications.
         * If it possible you should use {@link StorIOSQLite} methods instead
         * or call {@link #notifyAboutChanges(Changes)} manually.
         *
         * @return {@link SupportSQLiteOpenHelper}.
         */
        @NonNull
        public abstract SupportSQLiteOpenHelper sqliteOpenHelper();

    }
}
