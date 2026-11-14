/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.sova.metrics.collectors;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.GroupListing;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListGroupsResult;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import io.strimzi.sova.config.SovaConfig;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicKey;
import io.strimzi.sova.metrics.MetricSnapshot.ConsumerGroupTopicPartitionKey;
import io.strimzi.sova.metrics.collectors.ConsumerGroupCollector.ConsumerGroupData;
import io.strimzi.sova.metrics.collectors.TopicCollector.TopicData;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@code ConsumerGroupCollector}: the pure steps {@code computeLag} and
 * {@code requireUsableListing}, plus the failed-listing paths of {@code collect}.
 *
 * <p>The pure steps take plain records, so their scenarios are built directly. Only the
 * {@code collect} tests need an AdminClient, and they mock {@code listGroups} and
 * {@code describeCluster} — states a live cluster in the integration tests cannot easily be
 * pushed into. Expected values below are derived
 * from the invariants documented in {@code AGENTS.md} (negative lag is never clamped, {@code -1}
 * is synthesised for uncommitted partitions and excluded from the sums, a topic with no commits
 * produces no series, and so on) — not from running the collector and recording its output.
 */
class ConsumerGroupCollectorTest {
    private static final Predicate<String> ALL_TOPICS = topic -> true;

    /**
     * Every broker failed {@code listGroups}: an empty listing then means "we do not know", not
     * "there are no groups", so the cycle must fail like any other lookup in which every element
     * failed, rather than publish a snapshot without consumer group series.
     */
    @Test
    void listingThatFailedOnEveryBrokerFailsTheCycle() {
        RuntimeException cause = new RuntimeException("COORDINATOR_LOAD_IN_PROGRESS timed out");
        CollectionFailedException e = assertThrows(CollectionFailedException.class,
                () -> ConsumerGroupCollector.requireUsableListing(List.of(),
                        List.of(cause, new RuntimeException("b2"), new RuntimeException("b3")), 3));
        assertSame(cause, e.getCause());
    }

    /**
     * {@code listGroups} asks every broker separately, so an empty listing with one error can
     * also mean the brokers that did answer simply host no groups. That is not a failed lookup.
     */
    @Test
    void emptyListingWithSomeBrokersAnsweringIsKept() {
        assertDoesNotThrow(() -> ConsumerGroupCollector.requireUsableListing(
                List.of(), List.of(new RuntimeException("one broker down")), 3));
    }

    @Test
    void noGroupsAndNoErrorsIsAValidEmptyListing() {
        assertDoesNotThrow(() -> ConsumerGroupCollector.requireUsableListing(List.of(), List.of(), 3));
    }

    @Test
    void partialListingIsKept() {
        assertDoesNotThrow(() -> ConsumerGroupCollector.requireUsableListing(
                List.of(listing("g1")), List.of(new RuntimeException("one broker down")), 3));
    }

    /**
     * End to end through {@code collect}: when every broker failed the listing, the broker count
     * is looked up and the cycle fails instead of yielding an empty snapshot.
     */
    @Test
    void collectFailsWhenListingFailedOnEveryBroker() {
        RuntimeException cause = new RuntimeException("b1");
        Admin admin = stubAdmin(List.of(cause, new RuntimeException("b2")), 2);
        CollectionFailedException e = assertThrows(CollectionFailedException.class,
                () -> ConsumerGroupCollector.collect(admin, SovaConfig.forTesting("localhost:9092")));
        assertSame(cause, e.getCause());
    }

    /**
     * End to end through {@code collect}: one broker of three failed and the others host no
     * groups, so the cycle goes on with an empty group set.
     */
    @Test
    void collectKeepsEmptyListingWhenSomeBrokersAnswered() throws Exception {
        Admin admin = stubAdmin(List.of(new RuntimeException("one broker down")), 3);
        ConsumerGroupData data = ConsumerGroupCollector.collect(admin, SovaConfig.forTesting("localhost:9092"));
        assertTrue(data.members().isEmpty());
        assertTrue(data.committedOffsets().isEmpty());
    }

    /**
     * An empty listing without errors is the common "no groups" case: the broker count is not
     * needed, so {@code describeCluster} must not be called.
     */
    @Test
    void collectSkipsBrokerCountWhenListingHadNoErrors() throws Exception {
        Admin admin = stubAdmin(List.of(), 3);
        ConsumerGroupData data = ConsumerGroupCollector.collect(admin, SovaConfig.forTesting("localhost:9092"));
        assertTrue(data.members().isEmpty());
        verify(admin, never()).describeCluster();
    }

    /**
     * A mocked {@code Admin} whose {@code listGroups} returns no groups and the given per-broker
     * failures, and whose {@code describeCluster} reports {@code brokerCount} nodes.
     */
    private static Admin stubAdmin(List<Throwable> listingErrors, int brokerCount) {
        ListGroupsResult listing = mock(ListGroupsResult.class);
        when(listing.valid()).thenReturn(KafkaFuture.completedFuture(List.of()));
        when(listing.errors()).thenReturn(KafkaFuture.completedFuture(listingErrors));

        DescribeClusterResult cluster = mock(DescribeClusterResult.class);
        when(cluster.nodes()).thenReturn(KafkaFuture.completedFuture(Collections.nCopies(brokerCount, Node.noNode())));

        Admin admin = mock(Admin.class);
        when(admin.listGroups(any(ListGroupsOptions.class))).thenReturn(listing);
        when(admin.describeCluster()).thenReturn(cluster);
        return admin;
    }

    private static GroupListing listing(String groupId) {
        return new GroupListing(groupId, Optional.of(GroupType.CONSUMER), "consumer", Optional.of(GroupState.STABLE));
    }

    @Test
    void negativeLagIsReportedAsIsNeverClamped() {
        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp, new OffsetAndMetadata(100L))));
        TopicData topicData = topicData(Map.of("t", 1), Map.of(tp, 80L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicKey topicKey = new ConsumerGroupTopicKey("g", "t");
        assertEquals(-20L, result.lags().get(key),
                "a committed offset ahead of the high-water mark must be reported as a negative "
                        + "lag, not clamped to zero");
        assertEquals(-20L, result.lagSums().get(topicKey),
                "the sum of a single negative lag must stay negative");
        assertEquals(100L, result.currentOffsets().get(key));
    }

    @Test
    void uncommittedPartitionsAreSynthesisedAsMinusOneAndExcludedFromBothSums() {
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        TopicPartition tp2 = new TopicPartition("t", 2);
        // Only partition 0 is committed; the broker never reports 1 and 2 at all.
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp0, new OffsetAndMetadata(50L))));
        TopicData topicData = topicData(Map.of("t", 3),
                Map.of(tp0, 200L, tp1, 200L, tp2, 200L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key1 = new ConsumerGroupTopicPartitionKey("g", "t", "1");
        ConsumerGroupTopicPartitionKey key2 = new ConsumerGroupTopicPartitionKey("g", "t", "2");
        assertEquals(-1L, result.currentOffsets().get(key1),
                "an uncommitted partition must be synthesised as -1 in currentOffsets");
        assertEquals(-1L, result.currentOffsets().get(key2));
        assertEquals(-1L, result.lags().get(key1),
                "an uncommitted partition must be synthesised as -1 in lags");
        assertEquals(-1L, result.lags().get(key2));

        ConsumerGroupTopicKey topicKey = new ConsumerGroupTopicKey("g", "t");
        // Only partition 0 was really committed: offset 50, lag 200 - 50 = 150. Summing the two
        // synthesised -1s in would wrongly drag the sums down to 48 and 148.
        assertEquals(50L, result.currentOffsetSums().get(topicKey),
                "the -1s of uncommitted partitions must not be folded into the offset sum");
        assertEquals(150L, result.lagSums().get(topicKey),
                "the -1s of uncommitted partitions must not be folded into the lag sum");
    }

    @Test
    void topicTheGroupNeverCommittedToProducesNoSeriesAtAll() {
        TopicPartition committedTp = new TopicPartition("t1", 0);
        ConsumerGroupData groups = withCommitted(
                Map.of("g", Map.of(committedTp, new OffsetAndMetadata(10L))));
        // t2 exists in this cycle's topic data (5 partitions) but the group has never committed
        // to it at all.
        TopicData topicData = topicData(Map.of("t1", 1, "t2", 5), Map.of(committedTp, 20L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        assertTrue(result.currentOffsets().keySet().stream().noneMatch(k -> "t2".equals(k.topic())),
                "a topic with no commits must produce no currentOffsets series, got: "
                        + result.currentOffsets());
        assertTrue(result.lags().keySet().stream().noneMatch(k -> "t2".equals(k.topic())),
                "a topic with no commits must produce no lag series, got: " + result.lags());
        assertFalse(result.currentOffsetSums().containsKey(new ConsumerGroupTopicKey("g", "t2")));
        assertFalse(result.lagSums().containsKey(new ConsumerGroupTopicKey("g", "t2")));
        // The topic the group did commit to must still be reported normally.
        assertTrue(result.currentOffsets().containsKey(new ConsumerGroupTopicPartitionKey("g", "t1", "0")));
    }

    @Test
    void missingHighWaterMarkLeavesCurrentOffsetButNoLag() {
        TopicPartition tp = new TopicPartition("t", 0);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp, new OffsetAndMetadata(30L))));
        // No latest-offset entry for this partition at all this cycle.
        TopicData topicData = topicData(Map.of("t", 1), Map.of());

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicKey topicKey = new ConsumerGroupTopicKey("g", "t");
        assertEquals(30L, result.currentOffsets().get(key),
                "the committed value must still be recorded even without a high-water mark");
        assertFalse(result.lags().containsKey(key),
                "no lag entry must be produced when the high-water mark is missing");
        assertFalse(result.lagSums().containsKey(topicKey),
                "when every committed partition of a topic is missing its high-water mark, the "
                        + "lag sum entry must be absent entirely, not zero");
        assertEquals(30L, result.currentOffsetSums().get(topicKey),
                "the offset sum is unaffected by a missing high-water mark");
    }

    @Test
    void groupCommittedToATopicMissingFromThisCyclesTopicDataIsSkippedEntirely() {
        TopicPartition tp = new TopicPartition("ghost", 0);
        ConsumerGroupData groups = withCommitted(Map.of("g", Map.of(tp, new OffsetAndMetadata(40L))));
        // "ghost" is absent from topicPartitionCounts, as if deleted between listTopics and
        // describeTopics in this same cycle.
        TopicData topicData = topicData(Map.of(), Map.of());

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        assertTrue(result.currentOffsets().isEmpty(), "no partition count means nothing to emit");
        assertTrue(result.lags().isEmpty());
        assertTrue(result.currentOffsetSums().isEmpty());
        assertTrue(result.lagSums().isEmpty());
    }

    @Test
    void nullOffsetAndMetadataIsSkippedRatherThanRecordedAsAPhantomCommit() {
        TopicPartition tp0 = new TopicPartition("t", 0);
        TopicPartition tp1 = new TopicPartition("t", 1);
        // Map.of() rejects null values, so a mutable map is needed to reproduce what the admin
        // client can hand back.
        Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
        committed.put(tp0, new OffsetAndMetadata(15L));
        committed.put(tp1, null);
        ConsumerGroupData groups = withCommitted(Map.of("g", committed));
        TopicData topicData = topicData(Map.of("t", 2), Map.of(tp0, 100L, tp1, 100L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(groups, ALL_TOPICS, topicData);

        ConsumerGroupTopicPartitionKey key0 = new ConsumerGroupTopicPartitionKey("g", "t", "0");
        ConsumerGroupTopicPartitionKey key1 = new ConsumerGroupTopicPartitionKey("g", "t", "1");
        assertEquals(15L, result.currentOffsets().get(key0));
        assertEquals(-1L, result.currentOffsets().get(key1),
                "a null OffsetAndMetadata must be skipped and the partition synthesised as -1, "
                        + "never recorded as a real commit");
        assertEquals(-1L, result.lags().get(key1));
        assertEquals(15L, result.currentOffsetSums().get(new ConsumerGroupTopicKey("g", "t")),
                "the sum must not include a value derived from the null entry");
    }

    @Test
    void topicExcludePatternAppliesToCommittedOffsets() {
        TopicPartition tp = new TopicPartition("excluded-topic", 0);
        ConsumerGroupData groups = withCommitted(
                Map.of("g", Map.of(tp, new OffsetAndMetadata(99L))));
        TopicData topicData = topicData(Map.of("excluded-topic", 1), Map.of(tp, 200L));

        ConsumerGroupData result = ConsumerGroupCollector.computeLag(
                groups, topic -> !"excluded-topic".equals(topic), topicData);

        assertTrue(result.currentOffsets().isEmpty(),
                "commits on an excluded topic must produce no currentOffsets series, got: "
                        + result.currentOffsets());
        assertTrue(result.lags().isEmpty(),
                "commits on an excluded topic must produce no lag series, got: " + result.lags());
        assertTrue(result.currentOffsetSums().isEmpty());
        assertTrue(result.lagSums().isEmpty());
    }

    private static ConsumerGroupData withCommitted(
            Map<String, Map<TopicPartition, OffsetAndMetadata>> committedOffsets) {
        return new ConsumerGroupData(Map.of(), Map.of(), committedOffsets,
                Map.of(), Map.of(), Map.of(), Map.of());
    }

    private static TopicData topicData(Map<String, Integer> partitionCounts,
            Map<TopicPartition, Long> latestOffsets) {
        return new TopicData(partitionCounts, Map.of(), latestOffsets, Map.of());
    }

}
