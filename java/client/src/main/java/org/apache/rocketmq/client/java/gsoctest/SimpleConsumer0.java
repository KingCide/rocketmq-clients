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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class SimpleConsumer0 {
    private static final Logger log = LoggerFactory.getLogger(SimpleConsumer0.class);

    private SimpleConsumer0() {
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
        // 为了演示顺序和 ACK 失败处理，我们强制一次只拉取一条消息
        int maxMessageNum = 1; 
        // Set message invisible duration after it is received.
        Duration invisibleDuration = Duration.ofSeconds(15);
        // Receive message, multi-threading is more recommended for non-ordered messages.
        do {
            final List<MessageView> messages = consumer.receive(maxMessageNum, invisibleDuration);
            // 注意：如果 receive 失败（例如网络问题），这里也会抛出 ClientException，也需要处理
            log.info("Received {} message(s)", messages.size());
            for (MessageView message : messages) {
                final MessageId messageId = message.getMessageId();
                String extractedCount = "N/A"; // 初始化
                boolean ackSuccess = false; // 标记 ACK 是否成功
                try {
                    // 解析消息内容
                    ByteBuffer bodyBuffer = message.getBody();
                    byte[] bodyBytes = new byte[bodyBuffer.remaining()];
                    bodyBuffer.get(bodyBytes);
                    String messageBody = new String(bodyBytes, StandardCharsets.UTF_8);
                    
                    // 使用正则表达式提取数字
                    extractedCount = messageBody.replaceAll(".*RocketMQ (\\d+).*", "$1");
                    
                    // 模拟业务处理耗时
                    log.info("开始处理消息: [count={}] {}", extractedCount, messageBody);
                    Thread.sleep(5000); // 模拟处理

                    // 尝试 ACK
                    log.info("准备发送消息 ACK, count={}", extractedCount);
                    // 在这里可以模拟 ACK 失败，例如：
                    log.info("此时 ACK 应被 iptables 阻塞");
                    simulateAckLossByBlockingNetwork();
                    Thread.sleep(3000);
                    
                    consumer.ack(message);
                    ackSuccess = true; // 如果执行到这里，说明 ack() 调用本身没有抛异常
                    log.info("本地 ACK 调用成功 (不代表 Broker 已确认), count={}", extractedCount);
                   
                } catch (ClientException e) {
                    // ACK 调用本身抛出异常 (网络问题、超时、参数错误等)
                    log.error("ACK 消息失败 (ClientException), messageId={}, count={}", messageId, extractedCount, e);
                    // 在严格顺序场景下，这里应该停止处理该队列的后续消息，并实现重试逻辑
                    // 例如：可以记录 messageId，然后 continue 到下一个循环，下次 receive 可能会重投
                    // 或者更复杂的：等待一段时间后，尝试重新 ack(message)
                    // 这里为了简单，我们仅打印日志
                } catch (InterruptedException e) {
                    log.error("处理消息时线程被中断, messageId={}, count={}", messageId, extractedCount, e);
                    Thread.currentThread().interrupt(); // 重新设置中断状态
                    // 中断也意味着 ACK 未发送，需要考虑如何处理
                } catch (Throwable t) {
                    // 业务处理代码本身的异常 或 其他未预料的异常
                    log.error("处理消息或 ACK 时发生未知错误, messageId={}, count={}", messageId, extractedCount, t);
                    // 同样，ACK 很可能没有发送成功，需要考虑重试或等待重投
                }

                if (!ackSuccess) {
                    // 如果 ACK 未成功 (无论是 catch 了异常还是其他流程跳过)
                    // 在严格顺序模式下，你不应该继续这个 do-while 循环去 receive 下一条消息
                    // 你需要实现阻塞或等待重试/重投的逻辑
                    log.warn("ACK 未能成功执行 for messageId={}, count={}. 理论上应阻塞后续 receive 以保证顺序性。", messageId, extractedCount);
                    // **重要**: 当前的简单循环会继续执行 receive，这在 ACK 失败时会破坏严格顺序性！
                    // 实现真正的阻塞需要更复杂的逻辑（例如，为每个队列维护状态）
                }
            }
        } while (true);
        // Close the simple consumer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        // consumer.close();
    }

        /**
     * Executes the iptables script to temporarily block outgoing ACKs.
     *
     * @return true if the script adding the block rule executed successfully, false otherwise.
     */
    private static boolean simulateAckLossByBlockingNetwork() {
        log.warn("模拟 ACK 丢失：执行阻塞脚本 block_ack_iptables.sh...");
        // !!! Replace with the correct absolute path to your script !!!
        String scriptPath = "/home/vemu6/projects/kc-projects/gsoc/rocketmq/example/src/main/java/org/apache/rocketmq/example/poporderly/block_ack.sh";
        ProcessBuilder processBuilder = new ProcessBuilder("sudo", scriptPath);
        processBuilder.redirectErrorStream(true); // Merge stdout and stderr

        try {
            Process process = processBuilder.start();

            // Capture script output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                }
            }

            // Wait for the main script process to finish (the part that adds the rule).
            // The script launches the cleanup in the background.
            int exitCode = process.waitFor();

            if (exitCode == 0) {
                log.info("阻塞脚本执行成功 (exit code: {}). ACK 将在接下来几秒内被阻塞.", exitCode);
                log.debug("脚本输出:\n{}", output.toString()); // Log script output at debug level
                return true;
            } else {
                log.error("阻塞脚本执行失败! (exit code: {}).", exitCode);
                log.error("脚本输出:\n{}", output.toString());
                return false;
            }
        } catch (IOException e) {
            log.error("执行阻塞脚本时发生 IO 异常", e);
            return false;
        } catch (InterruptedException e) {
            log.error("等待阻塞脚本执行时被中断", e);
            Thread.currentThread().interrupt(); // Re-interrupt the thread
            return false;
        }
    }
}
