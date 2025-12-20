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

package org.casbin.watcher.example;

import org.casbin.jcasbin.main.Enforcer;
import org.casbin.watcher.RabbitMQWatcher;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Demo class showing how to use RabbitMQWatcher with jCasbin.
 * 
 * This example demonstrates:
 * 1. Creating a RabbitMQWatcher and connecting to RabbitMQ
 * 2. Integrating the watcher with a Casbin enforcer
 * 3. Setting up automatic policy reloading when updates are received
 * 4. Broadcasting policy updates to other instances
 */
public class RabbitMQWatcherDemo {

    /**
     * Example 1: Basic usage with default exchange
     */
    public static void basicExample() throws Exception {
        // Create model and policy files
        File modelFile = createModelFile();
        File policyFile = createPolicyFile();

        // Initialize the enforcer
        Enforcer enforcer = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());

        // Create watcher with RabbitMQ connection
        // Replace with your RabbitMQ server details
        RabbitMQWatcher watcher = new RabbitMQWatcher("amqp://localhost:5672");

        // Set the watcher on the enforcer
        enforcer.setWatcher(watcher);

        // Set up callback to reload policy when updates are received from other instances
        watcher.setUpdateCallback(() -> {
            System.out.println("Policy update received, reloading...");
            enforcer.loadPolicy();
        });

        // Make a policy change
        enforcer.addPolicy("alice", "data1", "read");
        enforcer.savePolicy();

        // Notify other instances about the policy update
        watcher.update();
        System.out.println("Policy update broadcasted to other instances");

        // Test the policy
        boolean allowed = enforcer.enforce("alice", "data1", "read");
        System.out.println("Alice can read data1: " + allowed);

        // Clean up
        watcher.close();
        modelFile.delete();
        policyFile.delete();
    }

    /**
     * Example 2: Using host and port instead of URI
     */
    public static void hostPortExample() throws Exception {
        File modelFile = createModelFile();
        File policyFile = createPolicyFile();

        Enforcer enforcer = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());

        // Connect using host and port
        RabbitMQWatcher watcher = new RabbitMQWatcher("localhost", 5672);

        enforcer.setWatcher(watcher);
        watcher.setUpdateCallback(() -> {
            enforcer.loadPolicy();
        });

        // Use the enforcer...
        enforcer.addPolicy("bob", "data2", "write");
        enforcer.savePolicy();
        watcher.update();

        // Clean up
        watcher.close();
        modelFile.delete();
        policyFile.delete();
    }

    /**
     * Example 3: Using custom exchange name
     */
    public static void customExchangeExample() throws Exception {
        File modelFile = createModelFile();
        File policyFile = createPolicyFile();

        Enforcer enforcer = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());

        // Use custom exchange name for isolation
        String customExchange = "my_app_casbin_updates";
        RabbitMQWatcher watcher = new RabbitMQWatcher("amqp://localhost:5672", customExchange);

        enforcer.setWatcher(watcher);
        watcher.setUpdateCallback(() -> {
            enforcer.loadPolicy();
        });

        // Use the enforcer...
        enforcer.addPolicy("carol", "data3", "delete");
        enforcer.savePolicy();
        watcher.update();

        // Clean up
        watcher.close();
        modelFile.delete();
        policyFile.delete();
    }

    /**
     * Example 4: Multi-instance scenario
     * This demonstrates how two separate instances can sync policies
     */
    public static void multiInstanceExample() throws Exception {
        File modelFile = createModelFile();
        File policyFile = createPolicyFile();

        // Instance 1
        Enforcer enforcer1 = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());
        RabbitMQWatcher watcher1 = new RabbitMQWatcher("amqp://localhost:5672");
        enforcer1.setWatcher(watcher1);
        watcher1.setUpdateCallback(() -> {
            System.out.println("Instance 1: Reloading policy...");
            enforcer1.loadPolicy();
        });

        // Instance 2 (simulating a different service/server)
        Enforcer enforcer2 = new Enforcer(modelFile.getAbsolutePath(), policyFile.getAbsolutePath());
        RabbitMQWatcher watcher2 = new RabbitMQWatcher("amqp://localhost:5672");
        enforcer2.setWatcher(watcher2);
        watcher2.setUpdateCallback(() -> {
            System.out.println("Instance 2: Reloading policy...");
            enforcer2.loadPolicy();
        });

        // Wait a bit for consumers to start
        Thread.sleep(500);

        System.out.println("Initial state - Instance 1 can alice read data1: " 
            + enforcer1.enforce("alice", "data1", "read"));
        System.out.println("Initial state - Instance 2 can alice read data1: " 
            + enforcer2.enforce("alice", "data1", "read"));

        // Update policy in instance 1
        System.out.println("\nAdding policy in Instance 1...");
        enforcer1.addPolicy("alice", "data1", "read");
        enforcer1.savePolicy();
        watcher1.update();  // This broadcasts to instance 2

        // Wait for instance 2 to receive the update
        Thread.sleep(1000);

        System.out.println("\nAfter update - Instance 1 can alice read data1: " 
            + enforcer1.enforce("alice", "data1", "read"));
        System.out.println("After update - Instance 2 can alice read data1: " 
            + enforcer2.enforce("alice", "data1", "read"));

        // Clean up
        watcher1.close();
        watcher2.close();
        modelFile.delete();
        policyFile.delete();
    }

    /**
     * Creates a temporary model file for the examples
     */
    private static File createModelFile() throws IOException {
        File modelFile = File.createTempFile("rbac_model", ".conf");
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
        return modelFile;
    }

    /**
     * Creates a temporary policy file for the examples
     */
    private static File createPolicyFile() throws IOException {
        File policyFile = File.createTempFile("rbac_policy", ".csv");
        try (FileWriter writer = new FileWriter(policyFile)) {
            writer.write("p, admin, data, read\n");
            writer.write("p, admin, data, write\n");
        }
        return policyFile;
    }

    /**
     * Main method to run the examples
     * Note: Requires a running RabbitMQ instance on localhost:5672
     */
    public static void main(String[] args) {
        try {
            System.out.println("=== RabbitMQ Watcher Demo ===\n");
            
            System.out.println("Running Example 1: Basic Usage");
            basicExample();
            
            System.out.println("\n\nRunning Example 2: Host and Port");
            hostPortExample();
            
            System.out.println("\n\nRunning Example 3: Custom Exchange");
            customExchangeExample();
            
            System.out.println("\n\nRunning Example 4: Multi-Instance Sync");
            multiInstanceExample();
            
            System.out.println("\n=== All examples completed ===");
        } catch (Exception e) {
            System.err.println("Error running examples: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
