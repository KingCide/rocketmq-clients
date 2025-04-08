package org.apache.rocketmq.client.java.gsoctest;

import java.nio.charset.StandardCharsets;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.message.Message;
import org.apache.rocketmq.client.apis.producer.Producer;
import org.apache.rocketmq.client.apis.producer.SendReceipt;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

public class producer {
    private static final Logger log = LoggerFactory.getLogger(producer.class);
    private static volatile boolean running = true;
    private static Producer producer;

    // 修改方法名称为 createMessage
    private static Message createMessage(byte[] body) throws ClientException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        String topic = "FifoTopic0";
        String tag = "MessageTag0";
        return provider.newMessageBuilder()
            .setTopic(topic)
            .setTag(tag)
            .setKeys("yourMessageKey-1ff69ada8e0e")
            .setMessageGroup("MessageGroup0")
            .setBody(body)
            .build();
    }

    public static void main(String[] args) throws InterruptedException, ClientException, IOException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();
        String endpoint = "127.0.0.1:8081";
        ClientConfiguration configuration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoint)
            .build();
        String topic = "FifoTopic0";
        
        producer = provider.newProducerBuilder()
            .setClientConfiguration(configuration)
            .setTopics(topic)
            .build();

        // 注册关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                log.info("收到关闭信号，开始关闭生产者...");
                running = false;
                
                if (producer != null) {
                    producer.close();
                    log.info("生产者已成功关闭");
                }
            } catch (Exception e) {
                log.error("关闭生产者时发生错误", e);
            }
        }));

        try {
            long startTime = System.currentTimeMillis();
            long endTime = startTime + 200000; // 100秒后结束
            int count = 0;
            
            // 使用running标志控制循环
            while (running && System.currentTimeMillis() < endTime) {
                count++;
                byte[] body = ("This is a FIFO message for Apache RocketMQ " + count)
                    .getBytes(StandardCharsets.UTF_8);
                Message message = createMessage(body);
                
                final SendReceipt sendReceipt = producer.send(message);
                log.info("Send message successfully, messageId={}", sendReceipt.getMessageId());
                Thread.sleep(3000);
            }
        } catch (InterruptedException e) {
            log.warn("发送消息被中断");
        } catch (ClientException e) {
            log.error("发送消息失败", e);
        } catch (Throwable t) {
            log.error("发送消息时发生未预期异常", t);
        }
    }
}
