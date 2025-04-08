package org.apache.rocketmq.client.java.gsoctest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.message.MessageId;
import org.apache.rocketmq.client.apis.message.MessageView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer1 {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer1.class);

    private SimpleConsumer1() {
    }

    @SuppressWarnings({"resource", "InfiniteLoopStatement"})
    public static void main(String[] args) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        String endpoints = "127.0.0.1:8081";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .build();
        String consumerGroup = "ConsumerGroup0";
        Duration awaitDuration = Duration.ofSeconds(30);
        String tag = "MessageTag0";
        String topic = "FifoTopic0";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        // In most case, you don't need to create too many consumers, singleton pattern is recommended.
        SimpleConsumer consumer = provider.newSimpleConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            // set await duration for long-polling.
            .setAwaitDuration(awaitDuration)
            // Set the subscription for the consumer.
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            .build();
        // Max message num for each long polling.
        int maxMessageNum = 1;
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(15);
        // Receive message, multi-threading is more recommended.
        int count = 0;
        do {
            final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
            //log.info("Received {} message(s)", messages.size());
            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                try {
                    // 解析消息内容
                    ByteBuffer bodyBuffer = message.getBody();
                    byte[] bodyBytes = new byte[bodyBuffer.remaining()];
                    bodyBuffer.get(bodyBytes);
                    String messageBody = new String(bodyBytes, StandardCharsets.UTF_8);
                    
                    // 使用正则表达式提取数字
                    String extractedCount = messageBody.replaceAll(".*RocketMQ (\\d+).*", "$1");
                    
                    // 模拟业务处理耗时
                    
                    log.info("开始处理消息: [count={}] ", extractedCount);
                    
                    Thread.sleep(1000);
                    consumer.ack(message);
                } catch (Throwable t) {
                    log.error("Message is failed to be acknowledged, messageId={}", messageId, t);
                }
            }
        } while (true);
        // Close the simple consumer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        // consumer.close();
    }
}
