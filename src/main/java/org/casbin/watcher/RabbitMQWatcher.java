// Copyright 2024 The casbin Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.casbin.watcher;

import com.rabbitmq.client.*;
import org.casbin.jcasbin.persist.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * RabbitMQWatcher implements the Watcher interface for jCasbin using RabbitMQ as the message broker.
 * It uses a fanout exchange to broadcast policy updates to all connected instances.
 * Each instance has a unique UUID to prevent processing its own updates (loop prevention).
 */
public class RabbitMQWatcher implements Watcher {
    private static final Logger logger = LoggerFactory.getLogger(RabbitMQWatcher.class);
    private static final String DEFAULT_EXCHANGE_NAME = "casbin_policy_updates";
    private static final String MESSAGE_SEPARATOR = ":";

    private final String instanceId;
    private final String exchangeName;
    private Connection connection;
    private Channel channel;
    private String queueName;
    private Runnable runnableCallback;
    private Consumer<String> consumerCallback;
    private volatile boolean running;

    /**
     * Creates a new RabbitMQWatcher with default exchange name.
     *
     * @param host RabbitMQ host
     * @param port RabbitMQ port
     * @throws IOException if connection fails
     * @throws TimeoutException if connection times out
     */
    public RabbitMQWatcher(String host, int port) throws IOException, TimeoutException {
        this(host, port, DEFAULT_EXCHANGE_NAME);
    }

    /**
     * Creates a new RabbitMQWatcher with custom exchange name.
     *
     * @param host RabbitMQ host
     * @param port RabbitMQ port
     * @param exchangeName custom exchange name
     * @throws IOException if connection fails
     * @throws TimeoutException if connection times out
     */
    public RabbitMQWatcher(String host, int port, String exchangeName) throws IOException, TimeoutException {
        this.instanceId = UUID.randomUUID().toString();
        this.exchangeName = exchangeName;
        this.running = false;
        
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        
        initializeConnection(factory);
    }

    /**
     * Creates a new RabbitMQWatcher using a connection URI.
     *
     * @param uri RabbitMQ connection URI (e.g., "amqp://guest:guest@localhost:5672")
     * @throws IOException if connection fails
     * @throws TimeoutException if connection times out
     */
    public RabbitMQWatcher(String uri) throws IOException, TimeoutException {
        this(uri, DEFAULT_EXCHANGE_NAME);
    }

    /**
     * Creates a new RabbitMQWatcher using a connection URI and custom exchange name.
     *
     * @param uri RabbitMQ connection URI
     * @param exchangeName custom exchange name
     * @throws IOException if connection fails
     * @throws TimeoutException if connection times out
     */
    public RabbitMQWatcher(String uri, String exchangeName) throws IOException, TimeoutException {
        this.instanceId = UUID.randomUUID().toString();
        this.exchangeName = exchangeName;
        this.running = false;
        
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
        } catch (Exception e) {
            throw new IOException("Failed to parse URI: " + uri, e);
        }
        
        initializeConnection(factory);
    }

    /**
     * Initializes the RabbitMQ connection, channel, exchange, and queue.
     *
     * @param factory configured ConnectionFactory
     * @throws IOException if connection fails
     * @throws TimeoutException if connection times out
     */
    private void initializeConnection(ConnectionFactory factory) throws IOException, TimeoutException {
        connection = factory.newConnection();
        channel = connection.createChannel();
        
        // Declare fanout exchange for broadcasting
        channel.exchangeDeclare(exchangeName, BuiltinExchangeType.FANOUT, true);
        
        // Create exclusive queue for this instance
        queueName = channel.queueDeclare().getQueue();
        
        // Bind queue to exchange
        channel.queueBind(queueName, exchangeName, "");
        
        logger.info("RabbitMQWatcher initialized with instance ID: {} on exchange: {}", 
                    instanceId, exchangeName);
    }

    /**
     * Sets the callback function to be called when a policy update is received.
     *
     * @param runnable callback to execute when update is received
     */
    @Override
    public void setUpdateCallback(Runnable runnable) {
        this.runnableCallback = runnable;
        this.consumerCallback = null;
        startConsuming();
    }

    /**
     * Sets the callback function to be called when a policy update is received.
     *
     * @param consumer callback to execute when update is received
     */
    @Override
    public void setUpdateCallback(Consumer<String> consumer) {
        this.consumerCallback = consumer;
        this.runnableCallback = null;
        startConsuming();
    }

    /**
     * Starts consuming messages from the queue.
     */
    private synchronized void startConsuming() {
        if (running) {
            return;
        }
        
        running = true;
        
        try {
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                try {
                    String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                    logger.debug("Received message: {}", message);
                    
                    // Parse message to extract instance ID
                    String senderId = extractSenderId(message);
                    
                    // Ignore messages from this instance (loop prevention)
                    if (!instanceId.equals(senderId)) {
                        logger.info("Processing update from instance: {}", senderId);
                        if (runnableCallback != null) {
                            runnableCallback.run();
                        } else if (consumerCallback != null) {
                            consumerCallback.accept(message);
                        }
                    } else {
                        logger.debug("Ignoring own update message");
                    }
                } catch (Exception e) {
                    logger.error("Error processing message", e);
                }
            };
            
            CancelCallback cancelCallback = consumerTag -> {
                logger.warn("Consumer cancelled: {}", consumerTag);
                running = false;
            };
            
            channel.basicConsume(queueName, true, deliverCallback, cancelCallback);
            logger.info("Started consuming messages from queue: {}", queueName);
            
        } catch (IOException e) {
            logger.error("Failed to start consuming messages", e);
            running = false;
        }
    }

    /**
     * Publishes a policy update notification to all watchers.
     */
    @Override
    public void update() {
        if (channel == null || !channel.isOpen()) {
            logger.error("Cannot publish update - channel is not open");
            return;
        }
        
        try {
            String message = createMessage();
            channel.basicPublish(exchangeName, "", null, message.getBytes(StandardCharsets.UTF_8));
            logger.info("Published update from instance: {}", instanceId);
        } catch (IOException e) {
            logger.error("Failed to publish update", e);
        }
    }

    /**
     * Creates a message with the instance ID.
     *
     * @return formatted message
     */
    private String createMessage() {
        return instanceId + MESSAGE_SEPARATOR + "update";
    }

    /**
     * Extracts the sender instance ID from a message.
     *
     * @param message the received message
     * @return sender instance ID
     */
    private String extractSenderId(String message) {
        if (message == null || message.isEmpty() || !message.contains(MESSAGE_SEPARATOR)) {
            return "";
        }
        String[] parts = message.split(MESSAGE_SEPARATOR, 2);
        return parts.length > 0 ? parts[0] : "";
    }

    /**
     * Closes the watcher and releases all resources.
     */
    public void close() {
        running = false;
        
        try {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            logger.error("Error closing channel", e);
        }
        
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            logger.error("Error closing connection", e);
        }
        
        logger.info("RabbitMQWatcher closed for instance: {}", instanceId);
    }

    /**
     * Gets the instance ID of this watcher.
     *
     * @return instance ID
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * Checks if the watcher is currently running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }
}
