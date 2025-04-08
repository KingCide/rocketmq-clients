/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.client.java.gsoctest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.apis.ClientServiceProvider;
import org.apache.rocketmq.client.apis.consumer.ConsumeResult;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.apis.consumer.FilterExpressionType;
import org.apache.rocketmq.client.apis.consumer.PushConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;


public class PushConsumer0 {
    private static final Logger log = LoggerFactory.getLogger(PushConsumer0.class);

    private PushConsumer0() {
    }

    public static void main(String[] args) throws ClientException, InterruptedException, IOException {
        final ClientServiceProvider provider = ClientServiceProvider.loadService();

        String endpoints = "127.0.0.1:8081";
        ClientConfiguration clientConfiguration = ClientConfiguration.newBuilder()
            .setEndpoints(endpoints)
            .build();
        String tag = "MessageTag0";
        FilterExpression filterExpression = new FilterExpression(tag, FilterExpressionType.TAG);
        String consumerGroup = "ConsumerGroup0";
        String topic = "FifoTopic0";
        // In most case, you don't need to create too many consumers, singleton pattern is recommended.
        PushConsumer pushConsumer = provider.newPushConsumerBuilder()
            .setClientConfiguration(clientConfiguration)
            // Set the consumer group name.
            .setConsumerGroup(consumerGroup)
            // Set the subscription for the consumer.
            .setSubscriptionExpressions(Collections.singletonMap(topic, filterExpression))
            // 设置消费线程数为1
            .setConsumptionThreadCount(1)
            .setMessageListener(messageView -> {
                try {
                    // 解析消息内容
                    ByteBuffer bodyBuffer = messageView.getBody();
                    byte[] bodyBytes = new byte[bodyBuffer.remaining()];
                    bodyBuffer.get(bodyBytes);
                    String messageBody = new String(bodyBytes, StandardCharsets.UTF_8);
                    
                    // 使用正则表达式提取数字
                    String extractedCount = messageBody.replaceAll(".*RocketMQ (\\d+).*", "$1");
                    
                    // 模拟业务处理耗时
                    log.info("开始处理消息: [count={}] {}", extractedCount, messageBody);
                    Thread.sleep(5000); // 模拟处理
                    
                    log.info("消息处理完成, count={}", extractedCount);
                    // 模拟 ACK 丢失
                    //log.info("此时 ACK 应被 iptables 阻塞");
                    //simulateAckLossByBlockingNetwork();
                    //Thread.sleep(3000);
                    return ConsumeResult.SUCCESS;
                } catch (InterruptedException e) {
                    log.error("处理消息时线程被中断", e);
                    Thread.currentThread().interrupt(); // 重新设置中断状态
                    return ConsumeResult.FAILURE;
                } catch (Throwable t) {
                    // 业务处理代码本身的异常 或 其他未预料的异常
                    log.error("处理消息时发生未知错误", t);
                    return ConsumeResult.FAILURE;
                }
            })
            .build();
        // Block the main thread, no need for production environment.
        Thread.sleep(Long.MAX_VALUE);
        // Close the push consumer when you don't need it anymore.
        // You could close it manually or add this into the JVM shutdown hook.
        pushConsumer.close();
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
