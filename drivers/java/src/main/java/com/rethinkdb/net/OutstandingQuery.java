package com.rethinkdb.net;

interface OutstandingQuery {
    void setError(String errMsg);
    long getToken();
}
