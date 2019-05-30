package com.pushtorefresh.storio3;

import androidx.annotation.NonNull;
import androidx.annotation.Nullable;

import com.pushtorefresh.storio3.internal.TypeMapping;

import java.util.Map;

/**
 * Interface for search type mapping.
 */
public interface TypeMappingFinder {
    @Nullable
    <T> TypeMapping<T> findTypeMapping(@NonNull Class<T> type);

    void directTypeMapping(@Nullable Map<Class<?>, ? extends TypeMapping<?>> directTypeMapping);

    @Nullable
    Map<Class<?>, ? extends TypeMapping<?>> directTypeMapping();
}
