# jCasbin RabbitMQ Watcher

[![CI](https://github.com/jcasbin/jcasbin-rabbitmq-watcher/workflows/CI/badge.svg)](https://github.com/jcasbin/jcasbin-rabbitmq-watcher/actions)
[![Maven Central](https://img.shields.io/maven-central/v/org.casbin/jcasbin-rabbitmq-watcher.svg)](https://mvnrepository.com/artifact/org.casbin/jcasbin-rabbitmq-watcher)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

RabbitMQ watcher for [jCasbin](https://github.com/casbin/jcasbin), enabling distributed policy synchronization across multiple instances using RabbitMQ as the message broker.

## Features

- **Distributed Policy Synchronization**: Automatically sync policy updates across multiple jCasbin instances
- **Loop Prevention**: Built-in UUID-based instance identification to prevent infinite update loops
- **Fanout Exchange**: Uses RabbitMQ fanout exchange for efficient broadcasting
- **Easy Integration**: Simple API compatible with jCasbin's Watcher interface
- **Testcontainers Support**: Comprehensive integration tests using Testcontainers for RabbitMQ
- **Production Ready**: Robust error handling and logging

## Installation

### Maven

```xml
<dependency>
    <groupId>org.casbin</groupId>
    <artifactId>jcasbin-rabbitmq-watcher</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

```gradle
implementation 'org.casbin:jcasbin-rabbitmq-watcher:1.0.0'
```

## Quick Start

### Basic Usage

```java
import org.casbin.jcasbin.main.Enforcer;
import org.casbin.watcher.RabbitMQWatcher;

// Create enforcer
Enforcer enforcer = new Enforcer("path/to/model.conf", "path/to/policy.csv");

// Create and set watcher
RabbitMQWatcher watcher = new RabbitMQWatcher("amqp://guest:guest@localhost:5672");
enforcer.setWatcher(watcher);

// The watcher will automatically reload policy when updates are received
watcher.setUpdateCallback(() -> {
    enforcer.loadPolicy();
});

// When you update policy, notify other instances
enforcer.addPolicy("alice", "data1", "read");
watcher.update();

// Close watcher when done
watcher.close();
```

### Using Host and Port

```java
RabbitMQWatcher watcher = new RabbitMQWatcher("localhost", 5672);
```

### Custom Exchange Name

```java
RabbitMQWatcher watcher = new RabbitMQWatcher(
    "amqp://guest:guest@localhost:5672",
    "my_custom_exchange"
);
```

For more detailed examples, see [RabbitMQWatcherDemo.java](src/test/java/org/casbin/watcher/example/RabbitMQWatcherDemo.java) which includes:
- Basic usage examples
- Multi-instance synchronization scenarios
- Custom exchange configuration
- Different connection methods

## How It Works

1. **Initialization**: Each `RabbitMQWatcher` instance creates a unique instance ID (UUID)
2. **Exchange Setup**: Declares a fanout exchange for broadcasting policy updates
3. **Queue Binding**: Creates an exclusive queue bound to the exchange
4. **Publishing**: When `update()` is called, publishes a message containing the instance ID
5. **Consuming**: Listens for messages on the queue
6. **Loop Prevention**: Ignores messages with matching instance ID
7. **Callback**: Triggers the update callback (typically to reload policy) for messages from other instances

## Architecture

```
┌─────────────┐         ┌──────────────────┐         ┌─────────────┐
│  Instance 1 │────────>│  RabbitMQ Fanout │────────>│  Instance 2 │
│  (Enforcer) │  update │     Exchange     │ message │  (Enforcer) │
└─────────────┘         └──────────────────┘         └─────────────┘
      ^                                                       │
      │                                                       │
      └───────────────────── loadPolicy ─────────────────────┘
```

Each instance:
- Publishes updates to the fanout exchange
- Receives updates from all other instances
- Filters out its own updates using instance ID
- Triggers policy reload on external updates

## Configuration

### Connection Options

The watcher supports multiple ways to connect to RabbitMQ:

#### URI-based Connection
```java
RabbitMQWatcher watcher = new RabbitMQWatcher("amqp://user:pass@host:5672/vhost");
```

#### Host and Port
```java
RabbitMQWatcher watcher = new RabbitMQWatcher("localhost", 5672);
```

### Exchange Configuration

By default, the watcher uses the exchange name `casbin_policy_updates`. You can customize this:

```java
RabbitMQWatcher watcher = new RabbitMQWatcher("amqp://localhost:5672", "my_exchange");
```

## Testing

This project includes comprehensive integration tests using [Testcontainers](https://www.testcontainers.org/), which automatically starts a RabbitMQ container for testing.

### Running Tests

```bash
mvn test
```

**Note**: Docker must be running for the tests to execute.

### Test Coverage

The test suite includes:
- Watcher creation and initialization
- Update callback functionality
- Loop prevention (instance ID filtering)
- Multi-instance synchronization
- Multiple updates handling
- Resource cleanup

## Requirements

- Java 8 or higher
- RabbitMQ server (3.x or higher)
- Docker (for running tests)

## Example: Multi-Instance Setup

```java
// Instance 1
Enforcer enforcer1 = new Enforcer("model.conf", "policy.csv");
RabbitMQWatcher watcher1 = new RabbitMQWatcher("amqp://localhost:5672");
enforcer1.setWatcher(watcher1);
watcher1.setUpdateCallback(() -> enforcer1.loadPolicy());

// Instance 2
Enforcer enforcer2 = new Enforcer("model.conf", "policy.csv");
RabbitMQWatcher watcher2 = new RabbitMQWatcher("amqp://localhost:5672");
enforcer2.setWatcher(watcher2);
watcher2.setUpdateCallback(() -> enforcer2.loadPolicy());

// Update policy in instance 1
enforcer1.addPolicy("alice", "data2", "write");
watcher1.update(); // This will trigger enforcer2 to reload

// Instance 2 now has the updated policy
boolean result = enforcer2.enforce("alice", "data2", "write"); // true
```

## Logging

The watcher uses SLF4J for logging. Configure your logging framework (e.g., Logback, Log4j2) to control log output.

Example Logback configuration:

```xml
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>

    <logger name="org.casbin.watcher" level="INFO"/>

    <root level="INFO">
        <appender-ref ref="STDOUT" />
    </root>
</configuration>
```

## API Reference

### RabbitMQWatcher

#### Constructors

- `RabbitMQWatcher(String host, int port)` - Connect using host and port
- `RabbitMQWatcher(String host, int port, String exchangeName)` - Connect with custom exchange
- `RabbitMQWatcher(String uri)` - Connect using AMQP URI
- `RabbitMQWatcher(String uri, String exchangeName)` - Connect using URI with custom exchange

#### Methods

- `void setUpdateCallback(Runnable callback)` - Set callback for policy updates
- `void update()` - Broadcast policy update to other instances
- `void close()` - Close connection and release resources
- `String getInstanceId()` - Get unique instance identifier
- `boolean isRunning()` - Check if watcher is running

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [jCasbin](https://github.com/casbin/jcasbin) - The amazing authorization library
- [RabbitMQ](https://www.rabbitmq.com/) - Robust messaging broker
- [Testcontainers](https://www.testcontainers.org/) - Testing with Docker containers