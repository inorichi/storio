package com.pushtorefresh.storio.basic_sample;

import android.arch.persistence.db.SupportSQLiteDatabase;
import android.arch.persistence.db.SupportSQLiteOpenHelper;
import android.support.annotation.NonNull;

public class DbOpenCallback extends SupportSQLiteOpenHelper.Callback {

    public DbOpenCallback() {
        super(1);
    }

    @Override
    public void onCreate(@NonNull SupportSQLiteDatabase db) {
        db.execSQL(TweetsTable.getCreateTableQuery());
    }

    @Override
    public void onUpgrade(@NonNull SupportSQLiteDatabase db, int oldVersion, int newVersion) {
        // no impl
    }
}
