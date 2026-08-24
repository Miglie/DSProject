package com.progetto.gossip;

import com.progetto.rmi.WorkerRemote;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Load-balancing decisions and version bookkeeping. Time is faked throughout: the service is built
 * with a controllable clock and rounds are driven by calling gossipRound() directly, so nothing
 * here waits on the real scheduler.
 */
class GossipServiceTest {

    private static final long T0 = 1_000_000L;

    /** Manually advanced clock, so a six-second threshold costs no wall-clock time to cross. */
    private static final class FakeClock implements LongSupplier {
        private long now = T0;

        @Override
        public long getAsLong() {
            return now;
        }

        void advance(long millis) {
            now += millis;
        }
    }

    private final FakeClock clock = new FakeClock();
    private final ConcurrentHashMap<String, WorkerRemote> peers = new ConcurrentHashMap<>();
    private final GossipService service = new GossipService("self", peers, clock);

    private void addPeer(String id) {
        peers.put(id, new StubPeer());
    }

    /**
     * Reads the service's own state. Handing exchangeState an empty ClusterState makes it a pure
     * read: there is nothing to adopt a version floor from and nothing to merge, so all it does is
     * return the local snapshot.
     */
    private WorkerView viewOf(String workerId) {
        return service.exchangeState(new ClusterState()).get(workerId);
    }

    /** Delivers a peer's view to the service the way a real gossip exchange would. */
    private void receiveGossip(String peerId, int load, long version) {
        ClusterState incoming = new ClusterState();
        incoming.merge(new WorkerView(peerId, load, version, clock.getAsLong()), clock.getAsLong());
        service.exchangeState(incoming);
    }

    // ---------- local load bookkeeping ----------

    @Test
    void recordLocalLoadChangeUpdatesTheLoadAndBumpsTheVersion() {
        long versionBefore = viewOf("self").getVersion();

        service.recordLocalLoadChange(1);
        service.recordLocalLoadChange(1);
        service.recordLocalLoadChange(-1);

        assertEquals(1, viewOf("self").getLoadCount());
        assertTrue(viewOf("self").getVersion() > versionBefore, "every load change must publish a newer version");
    }

    // ---------- forward target selection ----------

    @Test
    void noForwardTargetWhenThereAreNoPeers() {
        service.recordLocalLoadChange(10);

        assertNull(service.reserveForwardTarget());
    }

    @Test
    void noForwardTargetWhenTheImbalanceIsWithinTheThreshold() {
        addPeer("w2");
        receiveGossip("w2", 1, 1);
        service.recordLocalLoadChange(3); // difference is exactly 2, and the threshold is strict

        assertNull(service.reserveForwardTarget());
    }

    @Test
    void forwardsToAPeerOnceTheImbalanceExceedsTheThreshold() {
        addPeer("w2");
        receiveGossip("w2", 1, 1);
        service.recordLocalLoadChange(4);

        assertEquals("w2", service.reserveForwardTarget());
    }

    @Test
    void forwardsToTheLeastLoadedPeer() {
        addPeer("busy");
        addPeer("idle");
        receiveGossip("busy", 3, 1);
        receiveGossip("idle", 0, 1);
        service.recordLocalLoadChange(8);

        assertEquals("idle", service.reserveForwardTarget());
    }

    @Test
    void aPeerEvictedFromMembershipIsNotAForwardTarget() {
        addPeer("w2");
        receiveGossip("w2", 0, 1);
        service.recordLocalLoadChange(8);
        peers.remove("w2"); // evicted, but its view may still linger in the cluster state

        assertNull(service.reserveForwardTarget());
    }

    @Test
    void reservingASlotStopsABurstFromPilingOntoTheSamePeer() {
        // Gossip only refreshes a peer's load every couple of seconds. Without counting the jobs
        // already in flight towards it, every job of a burst would see the same "that peer is idle"
        // snapshot and they would all land on the same node.
        addPeer("w2");
        receiveGossip("w2", 1, 1);
        service.recordLocalLoadChange(4);

        assertEquals("w2", service.reserveForwardTarget());
        assertNull(service.reserveForwardTarget(),
                "the second job must account for the first one already on its way to w2");
    }

    @Test
    void releasingASlotMakesThePeerEligibleAgain() {
        addPeer("w2");
        receiveGossip("w2", 1, 1);
        service.recordLocalLoadChange(4);
        service.reserveForwardTarget();

        service.releaseForwardSlot("w2");

        assertEquals("w2", service.reserveForwardTarget());
    }

    @Test
    void forgetPeerDropsItFromTheClusterStateAndItsReservations() {
        addPeer("w2");
        receiveGossip("w2", 0, 1);
        service.recordLocalLoadChange(8);
        service.reserveForwardTarget();

        service.forgetPeer("w2");

        assertNull(viewOf("w2"));
    }

    // ---------- version floor (restart survival) ----------

    @Test
    void exchangeStateDoesNotImportOurOwnLoadFromAPeer() {
        ClusterState incoming = new ClusterState();
        incoming.merge(new WorkerView("self", 42, 999, T0), T0);

        service.exchangeState(incoming);

        assertEquals(0, viewOf("self").getLoadCount(), "a peer's copy of our own load must be ignored");
    }

    @Test
    void versionCounterIsLiftedAboveTheVersionPeersStillHoldOfUs() {
        // After a restart the counter is back near zero while peers still gossip a high-versioned
        // copy of the pre-crash entry. Without the floor, every update we publish would lose the
        // last-writer-wins comparison and our load would stay frozen at its old value forever.
        ClusterState incoming = new ClusterState();
        incoming.merge(new WorkerView("self", 7, 500, T0), T0);

        service.exchangeState(incoming);
        service.recordLocalLoadChange(1);

        assertTrue(viewOf("self").getVersion() > 500,
                "our post-restart updates must outrank the version peers remember");
        assertEquals(1, viewOf("self").getLoadCount());
    }

    // ---------- failure detection ----------

    @Test
    void aPeerIsEvictedOnceItsHeartbeatIsOlderThanTheThreshold() {
        addPeer("w2");
        receiveGossip("w2", 0, 1);

        // Regular, on-time rounds: no stall, and w2 never says anything again.
        for (int i = 0; i < 4; i++) {
            clock.advance(2000);
            service.gossipRound();
        }

        assertFalse(peers.containsKey("w2"), "a silent peer must be evicted from membership");
        assertNull(viewOf("w2"), "and from the gossiped load view");
    }

    @Test
    void aPeerThatKeepsGossipingIsNeverEvicted() {
        addPeer("w2");

        for (int i = 0; i < 10; i++) {
            clock.advance(2000);
            receiveGossip("w2", 0, i + 1);
            service.gossipRound();
        }

        assertTrue(peers.containsKey("w2"));
        assertNotNull(viewOf("w2"));
    }

    @Test
    void weAreNeverEvictedFromOurOwnView() {
        for (int i = 0; i < 10; i++) {
            clock.advance(2000);
            service.gossipRound();
        }

        assertNotNull(viewOf("self"));
    }

    @Test
    void aStalledRoundSkipsDetectionInsteadOfEvictingTheWholeCluster() {
        // Regression for the worst bug this code had: after a long freeze (SIGSTOP, a huge GC
        // pause, the machine suspending) every heartbeat looks ancient through no fault of the
        // peers. Judging them on those timestamps evicted the entire cluster in one round and left
        // the node permanently alone.
        addPeer("w2");
        addPeer("w3");
        receiveGossip("w2", 0, 1);
        receiveGossip("w3", 0, 1);

        clock.advance(20_000); // frozen
        service.gossipRound();

        assertTrue(peers.containsKey("w2"), "a stalled round must not evict anyone");
        assertTrue(peers.containsKey("w3"));
        assertEquals(T0 + 20_000, viewOf("w2").getLastHeartbeat(), "every heartbeat gets a fresh grace period");
    }

    @Test
    void theRoundAfterAStallDoesNotEvictEither() {
        addPeer("w2");
        receiveGossip("w2", 0, 1);

        clock.advance(20_000);
        service.gossipRound();
        clock.advance(2000);
        service.gossipRound();

        assertTrue(peers.containsKey("w2"),
                "forgiven heartbeats must survive the following on-time round");
    }

    @Test
    void forgivenessIsOnlyAGracePeriodNotAPermanentExemption() {
        // The stall handling must not become a way for genuinely dead peers to live forever.
        addPeer("w2");
        receiveGossip("w2", 0, 1);

        clock.advance(20_000);
        service.gossipRound();
        for (int i = 0; i < 4; i++) {
            clock.advance(2000);
            service.gossipRound();
        }

        assertFalse(peers.containsKey("w2"), "a peer still silent after the grace period must be evicted");
    }
}
