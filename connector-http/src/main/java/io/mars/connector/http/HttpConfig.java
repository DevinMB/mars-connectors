package io.mars.connector.http;

import java.io.Serializable;
import java.time.Duration;

/** Fully-resolved sink configuration, shipped to the task managers. */
final class HttpConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    final String baseUrl;
    final String method;
    final String authHeader;
    final String authValue;
    final String contentType;
    final int pathIndex;
    final int bodyIndex;
    final int methodIndex;
    final int maxRetries;
    final Duration timeout;
    final boolean dryRun;
    final boolean ignoreErrors;

    HttpConfig(String baseUrl, String method, String authHeader, String authValue,
               String contentType, int pathIndex, int bodyIndex, int methodIndex,
               int maxRetries, Duration timeout, boolean dryRun, boolean ignoreErrors) {
        this.baseUrl = baseUrl;
        this.method = method;
        this.authHeader = authHeader;
        this.authValue = authValue;
        this.contentType = contentType;
        this.pathIndex = pathIndex;
        this.bodyIndex = bodyIndex;
        this.methodIndex = methodIndex;
        this.maxRetries = maxRetries;
        this.timeout = timeout;
        this.dryRun = dryRun;
        this.ignoreErrors = ignoreErrors;
    }
}
