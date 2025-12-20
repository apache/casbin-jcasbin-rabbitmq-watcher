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

import org.casbin.jcasbin.main.Enforcer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for RabbitMQWatcher using Testcontainers.
 */
@Testcontainers
class RabbitMQWatcherTest {

    @Container
    private static final RabbitMQContainer rabbitMQContainer = new RabbitMQContainer(
            DockerImageName.parse("rabbitmq:3.12-alpine")
    );

    private RabbitMQWatcher watcher1;
    private RabbitMQWatcher watcher2;
    private File modelFile;
    private File policyFile;

    @BeforeEach
    void setUp() throws IOException {
        // Create temporary model and policy files for testing
        modelFile = File.createTempFile("model", ".conf");
        policyFile = File.createTempFile("policy", ".csv");

        // Write a simple RBAC model
        try (FileWriter writer = new FileWriter(modelFile)) {
            writer.write("[request_definition]\n");
            writer.write("r = sub, obj, act\n\n");
            writer.write("[policy_definition]\n");
            writer.write("p = sub, obj, act\n\n");
            writer.write("[role_definition]\n");
            writer.write("g = _, _\n\n");
            writer.write("[policy_effect]\n");
            writer.write("e = some(where (p.eft == allow))\n\n");
            writer.write("[matchers]\n");
            writer.write("m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act\n");
        }

        // Write initial policy
        try (FileWriter writer = new FileWriter(policyFile)) {
            writer.write("p, alice, data1, read\n");
            writer.write("p, bob, data2, write\n");
        }
    }

    @AfterEach
    void tearDown() {
        if (watcher1 != null) {
            watcher1.close();
        }
        if (watcher2 != null) {
            watcher2.close();
        }
        if (modelFile != null && modelFile.exists()) {
            modelFile.delete();
        }
        if (policyFile != null && policyFile.exists()) {
            policyFile.delete();
        }
    }

    @Test
    void testWatcherCreation() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        watcher1 = new RabbitMQWatcher(amqpUrl);

        assertNotNull(watcher1);
        assertNotNull(watcher1.getInstanceId());
        assertFalse(watcher1.getInstanceId().isEmpty());
    }

    @Test
    void testWatcherCreationWithHostAndPort() throws Exception {
        String host = rabbitMQContainer.getHost();
        int port = rabbitMQContainer.getAmqpPort();
        watcher1 = new RabbitMQWatcher(host, port);

        assertNotNull(watcher1);
        assertNotNull(watcher1.getInstanceId());
    }

    @Test
    void testWatcherCreationWithCustomExchange() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        String customExchange = "custom_casbin_exchange";
        watcher1 = new RabbitMQWatcher(amqpUrl, customExchange);

        assertNotNull(watcher1);
    }

    @Test
    void testUpdateCallback() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        watcher1 = new RabbitMQWatcher(amqpUrl);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        watcher1.setUpdateCallback(() -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        // Give consumer time to start
        Thread.sleep(500);

        // Create second watcher to send update
        watcher2 = new RabbitMQWatcher(amqpUrl);
        watcher2.update();

        // Wait for callback
        boolean received = latch.await(5, TimeUnit.SECONDS);
        assertTrue(received, "Callback should have been invoked");
        assertEquals(1, callbackCount.get(), "Callback should be called exactly once");
    }

    @Test
    void testLoopPrevention() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        watcher1 = new RabbitMQWatcher(amqpUrl);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callbackCount = new AtomicInteger(0);

        watcher1.setUpdateCallback(() -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        // Give consumer time to start
        Thread.sleep(500);

        // Send update from same watcher
        watcher1.update();

        // Wait a bit to see if callback is (incorrectly) invoked
        boolean received = latch.await(2, TimeUnit.SECONDS);
        assertFalse(received, "Callback should NOT be invoked for own updates");
        assertEquals(0, callbackCount.get(), "Callback should not be called for own messages");
    }

    @Test
    void testMultipleWatchersSynchronization() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        
        // Create two enforcers with watchers
        Enforcer enforcer1 = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());
        watcher1 = new RabbitMQWatcher(amqpUrl);
        enforcer1.setWatcher(watcher1);

        Enforcer enforcer2 = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());
        watcher2 = new RabbitMQWatcher(amqpUrl);
        enforcer2.setWatcher(watcher2);

        CountDownLatch latch = new CountDownLatch(1);

        // Set callback on second enforcer to reload policy
        watcher2.setUpdateCallback(() -> {
            enforcer2.loadPolicy();
            latch.countDown();
        });

        // Give consumers time to start
        Thread.sleep(500);

        // Verify initial state
        assertTrue(enforcer1.enforce("alice", "data1", "read"));
        assertFalse(enforcer1.enforce("alice", "data2", "write"));

        // Add policy to enforcer1 and save it
        enforcer1.addPolicy("alice", "data2", "write");
        enforcer1.savePolicy();

        // Trigger update
        watcher1.update();

        // Wait for enforcer2 to receive update
        boolean received = latch.await(5, TimeUnit.SECONDS);
        assertTrue(received, "Update should have been received");

        // Give a moment for the policy to be fully loaded after the callback
        Thread.sleep(100);

        // Verify enforcer2 has the new policy
        assertTrue(enforcer2.enforce("alice", "data2", "write"), 
                   "Enforcer2 should have the new policy after synchronization");
    }

    @Test
    void testMultipleUpdates() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        watcher1 = new RabbitMQWatcher(amqpUrl);
        watcher2 = new RabbitMQWatcher(amqpUrl);

        int expectedUpdates = 3;
        CountDownLatch latch = new CountDownLatch(expectedUpdates);
        AtomicInteger callbackCount = new AtomicInteger(0);

        watcher1.setUpdateCallback(() -> {
            callbackCount.incrementAndGet();
            latch.countDown();
        });

        // Give consumer time to start
        Thread.sleep(500);

        // Send multiple updates
        for (int i = 0; i < expectedUpdates; i++) {
            watcher2.update();
            Thread.sleep(100); // Small delay between updates
        }

        // Wait for all callbacks
        boolean received = latch.await(10, TimeUnit.SECONDS);
        assertTrue(received, "All updates should have been received");
        assertEquals(expectedUpdates, callbackCount.get(), 
                     "Should receive all " + expectedUpdates + " updates");
    }

    @Test
    void testDifferentInstances() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        watcher1 = new RabbitMQWatcher(amqpUrl);
        watcher2 = new RabbitMQWatcher(amqpUrl);

        assertNotEquals(watcher1.getInstanceId(), watcher2.getInstanceId(), 
                        "Different watcher instances should have different IDs");
    }

    @Test
    void testCloseWatcher() throws Exception {
        String amqpUrl = rabbitMQContainer.getAmqpUrl();
        watcher1 = new RabbitMQWatcher(amqpUrl);

        CountDownLatch latch = new CountDownLatch(1);
        watcher1.setUpdateCallback(latch::countDown);

        assertTrue(watcher1.isRunning());

        watcher1.close();
        assertFalse(watcher1.isRunning());

        // Verify watcher can be safely closed multiple times
        assertDoesNotThrow(() -> watcher1.close());
    }
}
