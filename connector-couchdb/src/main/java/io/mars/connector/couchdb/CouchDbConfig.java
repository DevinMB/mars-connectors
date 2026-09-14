package io.mars.connector.couchdb;

import java.io.Serializable;
import java.time.Duration;

/** Fully-resolved sink configuration, shipped to the task managers. */
final class CouchDbConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    final String url;
    final String db;
    final String username;
    final String password;
    final DocIdStrategy.Mode idMode;
    final String idField;
    final String idTemplate;
    final int docIndex;
    final int keyIndex;
    final int batchSize;
    final Duration flushInterval;
    final int maxRetries;
    final boolean ignoreWriteErrors;
    final Duration httpTimeout;

    CouchDbConfig(String url, String db, String username, String password,
                  DocIdStrategy.Mode idMode, String idField, String idTemplate,
                  int docIndex, int keyIndex, int batchSize, Duration flushInterval,
                  int maxRetries, boolean ignoreWriteErrors, Duration httpTimeout) {
        this.url = url;
        this.db = db;
        this.username = username;
        this.password = password;
        this.idMode = idMode;
        this.idField = idField;
        this.idTemplate = idTemplate;
        this.docIndex = docIndex;
        this.keyIndex = keyIndex;
        this.batchSize = batchSize;
        this.flushInterval = flushInterval;
        this.maxRetries = maxRetries;
        this.ignoreWriteErrors = ignoreWriteErrors;
        this.httpTimeout = httpTimeout;
    }
}
