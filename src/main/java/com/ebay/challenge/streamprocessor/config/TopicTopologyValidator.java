package com.ebay.challenge.streamprocessor.config;

import com.ebay.challenge.streamprocessor.exception.PartitionTopologyException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Fails fast if Kafka topic partitioning violates the join's co-partitioning contract.
 */
@Component
public class TopicTopologyValidator implements ApplicationRunner {

    private final String bootstrapServers;
    private final String adClicksTopic;
    private final String pageViewsTopic;
    private final int expectedPartitionCount;
    private final Duration timeout;

    public TopicTopologyValidator(
            @Value("${kafka.bootstrap-servers:localhost:29092}") String bootstrapServers,
            @Value("${kafka.topics.ad-clicks:ad_clicks}") String adClicksTopic,
            @Value("${kafka.topics.page-views:page_views}") String pageViewsTopic,
            @Value("${kafka.consumer.concurrency:3}") int expectedPartitionCount,
            @Value("${kafka.topology-validation.timeout-ms:10000}") long timeoutMs) {
        this.bootstrapServers = bootstrapServers;
        this.adClicksTopic = adClicksTopic;
        this.pageViewsTopic = pageViewsTopic;
        this.expectedPartitionCount = expectedPartitionCount;
        this.timeout = Duration.ofMillis(timeoutMs);
    }

    @Override
    public void run(ApplicationArguments args) {
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
            Map<String, TopicDescription> descriptions = admin
                    .describeTopics(List.of(adClicksTopic, pageViewsTopic))
                    .allTopicNames()
                    .get(timeout.toMillis(), TimeUnit.MILLISECONDS);

            int adClicksPartitions = descriptions.get(adClicksTopic).partitions().size();
            int pageViewsPartitions = descriptions.get(pageViewsTopic).partitions().size();

            if (adClicksPartitions != pageViewsPartitions) {
                throw topologyException("Topic partition counts differ: "
                        + adClicksTopic + "=" + adClicksPartitions + ", "
                        + pageViewsTopic + "=" + pageViewsPartitions + ".");
            }
            if (adClicksPartitions != expectedPartitionCount) {
                throw topologyException("Configured kafka.consumer.concurrency=" + expectedPartitionCount
                        + " but input topics have " + adClicksPartitions + " partitions.");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PartitionTopologyException("Interrupted while validating Kafka topic topology", e);
        } catch (ExecutionException e) {
            throw new PartitionTopologyException("Failed to validate Kafka topic topology for "
                    + adClicksTopic + " and " + pageViewsTopic + ": " + e.getCause().getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new PartitionTopologyException("Timed out after " + timeout.toMillis()
                    + "ms while validating Kafka topic topology for "
                    + adClicksTopic + " and " + pageViewsTopic, e);
        }
    }

    private PartitionTopologyException topologyException(String detail) {
        return new PartitionTopologyException(detail + " This processor treats numeric partition N across "
                + "ad_clicks and page_views as one join shard. Keep both topics keyed by user_id with the "
                + "same fixed partition count; for partition expansion, stop the processor, reset or migrate "
                + "state, update kafka.consumer.concurrency, and restart.");
    }
}
