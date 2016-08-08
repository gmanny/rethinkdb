package com.rethinkdb.net;

import com.rethinkdb.ast.Query;
import com.rethinkdb.gen.exc.ReqlRuntimeError;
import org.json.simple.JSONArray;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class AsyncCursor<T> implements OutstandingQuery {
    // public immutable members
    public final long token;

    // immutable members
    protected final Connection connection;
    protected final Query query;
    protected final boolean _isFeed;

    protected Optional<RuntimeException> error = Optional.empty();

    public AsyncCursor(Connection connection, Query query, Response firstResponse) {
        this.connection = connection;
        this.query = query;
        this.token = query.token;
        this._isFeed = firstResponse.isFeed();

        connection.addToCache(query.token, this);
    }

    public CompletableFuture<JSONArray> next() {
        if (error.isPresent()) {
            CompletableFuture<JSONArray> res = new CompletableFuture<>();
            res.completeExceptionally(error.get());
            return res;
        }

        return connection.continue_(this).thenApply(res -> res.data);
    }

    public CompletableFuture<Object> close() throws IOException {
        connection.removeFromCache(this.token);
        if (!error.isPresent()) {
            error = Optional.of(new NoSuchElementException());
            if (connection.isOpen()) {
                return connection.stopAsync(this);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    public void setError(String errMsg) {
        if(!error.isPresent()){
            error = Optional.of(new ReqlRuntimeError(errMsg));
        }
    }

    @Override
    public long getToken() {
        return token;
    }
}
