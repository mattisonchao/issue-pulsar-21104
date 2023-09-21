package io.github.mattisonchao;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.UUID;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@Test
public class TestIssue21104 {
    private static final Logger logger = LoggerFactory.getLogger(TestIssue21104.class);
    private PulsarContainer pulsar;
    private String payload30k;

    @BeforeTest
    private void initialize() {
        pulsar = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:3.1.0"));
        pulsar.start();
        final var strBuilder = new StringBuilder();
        final var targetSize = 30 * 1024; // 30kb
        while (strBuilder.length() < targetSize) {
            strBuilder.append(UUID.randomUUID());
        }
        payload30k = strBuilder.substring(0, targetSize);
    }


    @Test
    public void testSubscriptionBlock() throws PulsarClientException, PulsarAdminException {
        final var topicName = "persistent://public/default/subscriptionBlock";
        final var subName = "subscription-1";
        final var publishedMessage = new HashSet<String>();
        final var consumedMessage = new HashSet<String>();
        final var admin = PulsarAdmin.builder()
                .serviceHttpUrl(pulsar.getHttpServiceUrl())
                .build();
        // (1) Create 3 partitioned topic
        logger.info("Step - 1");
        admin.topics().createPartitionedTopic(topicName, 2);
        admin.topics().createSubscription(topicName, subName, MessageId.earliest);

        // (2) Publish 2 min messages (30kb)
        logger.info("Step - 2");
        final var client = PulsarClient.builder()
                .serviceUrl(pulsar.getPulsarBrokerUrl())
                .build();

        final var producer = client.newProducer()
                .topic(topicName)
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(8)  // Support 5 MB files
                .blockIfQueueFull(true) // Support 5 MB files
                .batchingMaxBytes(5242880)
                .create();
        final var startTime = currentTimeMillis();
        var messageGenerator = 0;
        while ((currentTimeMillis() - startTime) < MINUTES.toMillis(2)) {
            final var key = messageGenerator + "";
            final var message = messageGenerator + payload30k;
            producer.newMessage()
                    .key(key)
                    .value(message.getBytes(StandardCharsets.UTF_8))
                    .send();
            publishedMessage.add(key);
            messageGenerator++;
        }

        // (3) consume messages
        logger.info("Step - 3");
        final var consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName(subName)
                .subscriptionType(SubscriptionType.Shared)
                .receiverQueueSize(8)
                .ackTimeout(5, SECONDS)
                .subscribe();

        while (!consumedMessage.containsAll(publishedMessage)) {
            final var message = consumer.receive();
            final var key = message.getKey();
            consumedMessage.add(key);
            consumer.acknowledge(message);
        }
        logger.info("Success - ");
        logger.info("published messages : {}", publishedMessage.size());
        logger.info("consumed messages : {}", consumedMessage.size());
        // cleanup
        admin.close();
        client.close();
        logger.info("Exit - ");
    }

}
