package com.commercetools.sdk.jvm.spring.batch.item;

import io.sphere.sdk.client.BlockingSphereClient;
import io.sphere.sdk.models.Base;
import io.sphere.sdk.models.SphereException;
import io.sphere.sdk.queries.QueryDsl;
import io.sphere.sdk.queries.QueryPredicate;
import io.sphere.sdk.queries.QuerySort;
import org.springframework.batch.item.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Function;

import static java.lang.String.format;

final class SortByIdQueryItemReader<T, C extends QueryDsl<T, C>> extends Base implements ItemStreamReader<T> {
    public static final String LAST_ID_RETURNED_EXECUTION_CONTEXT_KEY = "lastIdReturned";
    private String lastIdReturned;
    private String lastIdBuffer;
    private final BlockingSphereClient client;
    private QueryDsl<T, C> query;
    private final Function<T, String> idExtractor;
    private Queue<T> buffer = new LinkedList<>();

    //TODO maybe use public static of method
    //TODO alias with T extends Identifiable<T>
    public SortByIdQueryItemReader(final BlockingSphereClient client, final QueryDsl<T, C> query, final Function<T, String> idExtractor) {
        this.client = client;
        this.query = query.withSort(QuerySort.of("id asc")).withFetchTotal(false);
        this.idExtractor = idExtractor;
    }

    @Override
    public T read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        final T value;
        if (!buffer.isEmpty()) {
            value = buffer.poll();
        } else {
            final List<T> results = queryNewElements();
            if (results.isEmpty()) {
                value = null;
            } else {
                final T lastElement = results.get(results.size() - 1);
                lastIdBuffer = idExtractor.apply(lastElement);
                buffer.addAll(results);
                value = buffer.poll();
            }
        }
        lastIdReturned = value != null ? idExtractor.apply(value) : null;
        return value;
    }

    private List<T> queryNewElements() {
        final List<T> results;
        try {
            results = client.executeBlocking(nextQuery()).getResults();
        } catch (final SphereException e) {
            throw new NonTransientResourceException("An error occurred during a query.", e);
        }
        return results;
    }

    private QueryDsl<T, C> nextQuery() {
        return lastIdBuffer == null ? query : query.plusPredicates(QueryPredicate.of(format("id > \"%s\"", lastIdBuffer)));
    }

    @Override
    public void open(final ExecutionContext executionContext) throws ItemStreamException {
        lastIdBuffer = executionContext.getString(LAST_ID_RETURNED_EXECUTION_CONTEXT_KEY, null);
        lastIdReturned = lastIdBuffer;
    }

    @Override
    public void update(final ExecutionContext executionContext) throws ItemStreamException {
        executionContext.putString(LAST_ID_RETURNED_EXECUTION_CONTEXT_KEY, lastIdReturned);
    }

    @Override
    public void close() throws ItemStreamException {

    }
}
