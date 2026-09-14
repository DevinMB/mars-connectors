package io.mars.connector.ha;

import java.io.Serializable;
import java.time.Duration;

/** Fully-resolved sink configuration, shipped to the task managers. */
final class HaConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    final String baseUrl;
    final String token;
    final String domain;
    final int actionIndex;
    final int entityIndex;
    final int attributesIndex;
    final boolean restorePrevious;
    final boolean dryRun;
    final boolean ignoreErrors;
    final Duration timeout;

    HaConfig(String baseUrl, String token, String domain, int actionIndex, int entityIndex,
             int attributesIndex, boolean restorePrevious, boolean dryRun,
             boolean ignoreErrors, Duration timeout) {
        this.baseUrl = baseUrl;
        this.token = token;
        this.domain = domain;
        this.actionIndex = actionIndex;
        this.entityIndex = entityIndex;
        this.attributesIndex = attributesIndex;
        this.restorePrevious = restorePrevious;
        this.dryRun = dryRun;
        this.ignoreErrors = ignoreErrors;
        this.timeout = timeout;
    }
}
