package com.progetto.gossip;

import com.progetto.rmi.WorkerRemote;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Merge rules of the gossip state. These are the load-balancing and failure-detection foundations:
 * which view wins, and when a heartbeat counts as refreshed.
 */
class ClusterStateTest {

    private static final long T0 = 1_000_000L;

    private static ConcurrentHashMap<String, WorkerRemote> peers(String... ids) {
        ConcurrentHashMap<String, WorkerRemote> map = new ConcurrentHashMap<>();
        for (String id : ids) {
            map.put(id, new StubPeer());
        }
        return map;
    }

    private static ClusterState stateWith(WorkerView... views) {
        ClusterState state = new ClusterState();
        for (WorkerView view : views) {
            state.merge(view, view.getLastHeartbeat());
        }
        return state;
    }

    @Test
    void mergeAppliesAStrictlyNewerVersion() {
        ClusterState state = stateWith(new WorkerView("w1", 3, 1, T0));

        state.merge(new WorkerView("w1", 7, 2, T0), T0);

        assertEquals(7, state.get("w1").getLoadCount());
        assertEquals(2, state.get("w1").getVersion());
    }

    @Test
    void mergeRejectsAnOlderVersion() {
        ClusterState state = stateWith(new WorkerView("w1", 7, 5, T0));

        state.merge(new WorkerView("w1", 99, 4, T0), T0);

        assertEquals(7, state.get("w1").getLoadCount(), "an older view must not overwrite a newer one");
    }

    @Test
    void mergeRejectsTheSameVersion() {
        ClusterState state = stateWith(new WorkerView("w1", 7, 5, T0));

        state.merge(new WorkerView("w1", 99, 5, T0), T0);

        assertEquals(7, state.get("w1").getLoadCount(), "last-writer-wins is strict: equal versions do not win");
    }

    @Test
    void mergeStampsTheHeartbeatWithTheLocalClockNotTheSendersTimestamp() {
        // Heartbeat ages are compared against our own clock only, so the cluster needs no clock
        // synchronisation. A peer with a wildly wrong clock must not be able to look fresh (or dead).
        ClusterState state = new ClusterState();

        state.merge(new WorkerView("w1", 0, 1, 999_999_999_999L), T0);

        assertEquals(T0, state.get("w1").getLastHeartbeat());
    }

    @Test
    void aRelayedStaleViewDoesNotRefreshTheHeartbeat() {
        // This is precisely what makes failure detection work: peers keep relaying a dead node's
        // last known view forever, and re-seeing it must not make that node look alive again.
        ClusterState state = stateWith(new WorkerView("dead", 0, 5, T0));

        state.merge(new WorkerView("dead", 0, 5, T0), T0 + 60_000);

        assertEquals(T0, state.get("dead").getLastHeartbeat(),
                "re-receiving an already-known version must leave the heartbeat untouched");
    }

    @Test
    void mergeAllNeverImportsOurOwnEntry() {
        // Regression: a worker is the only authority on its own load. Adopting a peer's copy of it
        // would publish a stale load and, after a restart, pin the entry at a version our own
        // updates could never beat.
        ClusterState local = stateWith(new WorkerView("self", 0, 1, T0));
        ClusterState incoming = stateWith(new WorkerView("self", 42, 999, T0));

        local.mergeAll(incoming, "self", peers(), T0);

        assertEquals(0, local.get("self").getLoadCount());
        assertEquals(1, local.get("self").getVersion());
    }

    @Test
    void mergeAllIgnoresWorkersThatAreNotKnownPeers() {
        // Keeps eviction sticky: an evicted worker must not sneak back in as relayed hearsay from a
        // third party. It gets back only by announcing itself (see Worker.exchangeState).
        ClusterState local = stateWith(new WorkerView("self", 0, 1, T0));
        ClusterState incoming = stateWith(new WorkerView("evicted", 5, 9, T0));

        local.mergeAll(incoming, "self", peers(), T0);

        assertNull(local.get("evicted"));
    }

    @Test
    void mergeAllAppliesViewsOfKnownPeers() {
        ClusterState local = stateWith(new WorkerView("self", 0, 1, T0));
        ClusterState incoming = stateWith(new WorkerView("w2", 5, 9, T0));

        local.mergeAll(incoming, "self", peers("w2"), T0 + 500);

        assertNotNull(local.get("w2"));
        assertEquals(5, local.get("w2").getLoadCount());
        assertEquals(T0 + 500, local.get("w2").getLastHeartbeat());
    }

    @Test
    void touchAllRestampsEveryEntryWithoutChangingLoadOrVersion() {
        ClusterState state = stateWith(
                new WorkerView("w1", 3, 7, T0),
                new WorkerView("w2", 4, 8, T0));

        state.touchAll(T0 + 20_000);

        assertEquals(T0 + 20_000, state.get("w1").getLastHeartbeat());
        assertEquals(T0 + 20_000, state.get("w2").getLastHeartbeat());
        assertEquals(3, state.get("w1").getLoadCount());
        assertEquals(7, state.get("w1").getVersion());
    }

    @Test
    void snapshotIsADefensiveCopy() {
        ClusterState state = stateWith(new WorkerView("w1", 1, 1, T0));

        ClusterState snapshot = state.snapshot();
        state.merge(new WorkerView("w1", 99, 2, T0), T0);

        assertEquals(1, snapshot.get("w1").getLoadCount(), "the snapshot must not track later local changes");
    }

    @Test
    void removeDropsTheWorkerEntirely() {
        ClusterState state = stateWith(new WorkerView("w1", 1, 1, T0));

        state.remove("w1");

        assertNull(state.get("w1"));
        assertEquals(0, state.size());
    }
}
