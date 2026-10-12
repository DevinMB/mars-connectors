package io.mars.connector.couchdb;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A query that joins or aggregates updating data retracts a row and then writes its
 * replacement. The planner refuses to send that to a sink that does not accept
 * DELETE, which ruled out exactly the current-state projections CouchDB is good at.
 * sink.delete-mode=ignore accepts them; the delete is dropped, so a replay still
 * never removes a document.
 */
class DeleteModeTest {

    private static CouchDbConfig config(boolean ignoreDeletes) {
        return new CouchDbConfig("http://couch:5984", "db", null, null,
                DocIdStrategy.Mode.CUSTOM, "", "latest:{doc.stationId}", 1, 0, 200,
                Duration.ofSeconds(2), 5, false, Duration.ofSeconds(30), ignoreDeletes, false);
    }

    @Test
    @DisplayName("by default a plan that can delete is refused, as before")
    void rejectsDeletesByDefault() {
        ChangelogMode mode = new CouchDbDynamicTableSink(config(false)).getChangelogMode(ChangelogMode.all());
        assertTrue(mode.contains(RowKind.INSERT) && mode.contains(RowKind.UPDATE_AFTER));
        assertFalse(mode.contains(RowKind.DELETE));
    }

    @Test
    @DisplayName("with delete-mode=ignore, joins and aggregations are accepted")
    void acceptsDeletesWhenIgnoring() {
        ChangelogMode mode = new CouchDbDynamicTableSink(config(true)).getChangelogMode(ChangelogMode.all());
        assertTrue(mode.contains(RowKind.DELETE));
        // Upsert, not retract: the replacement overwrites by _id, so no UPDATE_BEFORE.
        assertFalse(mode.contains(RowKind.UPDATE_BEFORE));
    }

    @Test
    @DisplayName("retractions are never written")
    void retractionsAreDropped() {
        assertTrue(CouchDbSinkWriter.isRetraction(RowKind.DELETE));
        assertTrue(CouchDbSinkWriter.isRetraction(RowKind.UPDATE_BEFORE));
        assertFalse(CouchDbSinkWriter.isRetraction(RowKind.INSERT));
        assertFalse(CouchDbSinkWriter.isRetraction(RowKind.UPDATE_AFTER));
    }
}
