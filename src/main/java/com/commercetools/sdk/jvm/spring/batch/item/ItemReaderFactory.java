package com.commercetools.sdk.jvm.spring.batch.item;

import io.sphere.sdk.client.BlockingSphereClient;
import io.sphere.sdk.models.Identifiable;
import io.sphere.sdk.queries.QueryDsl;
import org.springframework.batch.item.ItemStreamReader;

import java.util.function.Function;

public final class ItemReaderFactory {
    private ItemReaderFactory() {
    }

    public static <T, C extends QueryDsl<T, C>> ItemStreamReader<T> sortedByIdQueryReader(final BlockingSphereClient client, final QueryDsl<T, C> query, final Function<T, String> idExtractor) {
        return new SortByIdQueryItemReader<>(client, query, idExtractor);
    }

    public static <T extends Identifiable<T>, C extends QueryDsl<T, C>> ItemStreamReader<T> sortedByIdQueryReader(final BlockingSphereClient client, final QueryDsl<T, C> query) {
        return sortedByIdQueryReader(client, query, Identifiable::getId);
    }
}
