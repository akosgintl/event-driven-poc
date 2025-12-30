# Event-Driven Microservices POC Architecture

## Overview

This POC demonstrates a simple event-driven microservices architecture for order processing, integrating Redis, Kafka, Elasticsearch, and PostgreSQL.

## Architecture Diagram

```
┌─────────────────┐
│   API Gateway   │ (Future: for now, direct service calls)
└────────┬────────┘
         │
         v
┌─────────────────────────────────────────────────────────────┐
│                     Order Service                            │
│  ┌──────────┐           ┌──────────────┐                    │
│  │ REST API │──────────>│ PostgreSQL   │                    │
│  └────┬─────┘           │ (orders_db)  │                    │
│       │                 └──────────────┘                    │
│       │                                                      │
│       │ publishes                                           │
│       v                                                      │
│  ┌──────────────────────────────────────┐                  │
│  │        Kafka Topic                   │                  │
│  │     "order.created"                  │                  │
│  │     "order.confirmed"                │                  │
│  │     "order.cancelled"                │                  │
│  └──────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────┘
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         v                 v                 v
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Inventory     │ │  Notification   │ │     Search      │
│    Service      │ │    Service      │ │    Service      │
│                 │ │                 │ │                 │
│ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────┐ │
│ │ PostgreSQL  │ │ │ │    Redis    │ │ │ │Elasticsearch│ │
│ │(inventory_db│ │ │ │  (cache)    │ │ │ │  (search)   │ │
│ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────┘ │
│                 │ │                 │ │                 │
│ • Reserve stock │ │ • Cache user    │ │ • Index orders  │
│ • Update qty    │ │   preferences   │ │ • Full-text     │
│                 │ │ • Send notifs   │ │   search        │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

## 📦 Project Structure

```
event-driven-poc/
├── docker-compose.yml              # Infrastructure orchestration
├── README.md                       # Comprehensive documentation
├── .gitignore
├── .python-version
├── pyproject.toml
├── requirements.txt
├── uv.lock
│
├── docs/
│   ├── ARCHITECTURE.md             # Architecture details + concept mapping
│   └── LEARNING-ROADMAP.md         # Your path from POC to production
│
├── scripts/
│   ├── start.sh                    # Quick start script
│   └── test.sh                     # Automated testing
│
├── shared/
│   └── events.py                   # Shared event models (CloudEvents spec)
│
├── order-service/                  # FastAPI + PostgreSQL
│   ├── main.py                     # REST API + Kafka producer
│   ├── requirements.txt
│   └── Dockerfile
│
├── inventory-service/              # Python + PostgreSQL
│   ├── main.py                     # Kafka consumer + stock management
│   ├── requirements.txt
│   └── Dockerfile
│
├── notification-service/           # Python + Redis
│   ├── main.py                     # Kafka consumer + Redis caching
│   ├── requirements.txt
│   └── Dockerfile
│
└── search-service/                 # Python + Elasticsearch
    ├── main.py                     # Kafka consumer + search indexing
    ├── requirements.txt
    └── Dockerfile
```

## Component Responsibilities

### Order Service
- **Action**: Stores in database → Publishes events to Kafka
- **Primary Database**: PostgreSQL (orders_db)
- **Responsibilities**:
  - Accept new orders via REST API
  - Persist order data
  - Publish domain events to Kafka
- **Events Published**:
  - `order.created` - New order received
  - `order.confirmed` - Payment confirmed
  - `order.cancelled` - Order cancelled

### Inventory Service
- **Primary Database**: PostgreSQL (inventory_db)
- **Responsibilities**:
  - Consume order events from Kafka
  - Check and reserve inventory
  - Update stock quantities
- **Events Consumed**:
  - `order.created` - Reserve inventory
  - `order.cancelled` - Release inventory

### Notification Service
- **Cache**: Redis
- **Responsibilities**:
  - Consume order events from Kafka
  - Cache user notification preferences
  - Send notifications (email/SMS simulation)
- **Events Consumed**:
  - `order.created` - Notify customer
  - `order.confirmed` - Notify confirmation
  - `order.cancelled` - Notify cancellation

### Search Service
- **Search Engine**: Elasticsearch
- **Responsibilities**:
  - Consume order events from Kafka
  - Index orders for full-text search
  - Provide search API (future enhancement)
- **Events Consumed**:
  - `order.created` - Index new order
  - `order.confirmed` - Update order status
  - `order.cancelled` - Update order status

## Distributed Systems Concepts Applied

### 1. CAP Theorem Tradeoffs
- **PostgreSQL (CP)**: Strong consistency for transactional data
  - Order and inventory data require ACID guarantees
  - Configured with synchronous replication in production
- **Elasticsearch (AP)**: Eventual consistency for search
  - Search can tolerate slight staleness
  - Prioritizes availability and low latency
- **Redis (CP with Sentinel)**: Strong consistency for critical cache
  - User preferences cached with TTL
  - Can fall back to database on cache miss

### 2. Event-Driven Architecture (Saga Pattern - Choreography)
- **Choreography-based saga**: Services react to events independently
- **Compensating transactions**: If inventory reservation fails, publish `inventory.unavailable` event
- **Eventual consistency**: System converges to consistent state over time
- **No distributed transactions**: Each service has local transactions only

### 3. Partitioning & Scaling
- **Kafka Topics**: Partitioned by order_id for parallel processing
  - Hash-based partitioning ensures order events for same order go to same partition
  - Maintains ordering guarantees per order
- **PostgreSQL**: Can partition by date or customer_id for time-series queries
- **Elasticsearch**: Auto-sharded indexes for horizontal scaling
- **Redis**: Can use Redis Cluster for hash slot partitioning (16,384 slots)

### 4. Replication Patterns
- **Kafka**: Leader-follower replication per partition
  - Replication factor of 1 (single node) in this POC
  - Production: RF=3 for fault tolerance
- **PostgreSQL**: Supports streaming replication (leader-follower)
  - Single instance in POC
  - Production: Primary + replicas with automatic failover
- **Elasticsearch**: Replicates shards across nodes
  - Single node in POC (discovery.type=single-node)
  - Production: Multi-node cluster with replica shards

### 5. Service Discovery & Health Checks
- **Docker DNS**: Services discover each other by container name
- **Health checks**: Each service has Docker healthcheck probes
  - Liveness: Is the service running?
  - Readiness: Can it handle traffic?
- **Future**: Consul or Kubernetes for dynamic service discovery

### 6. Fault Tolerance Patterns
- **Circuit Breaker**: Implement in services calling external APIs
- **Retry with Backoff**: Kafka consumer retries on transient failures
- **Dead Letter Queue**: Failed events moved to DLQ topic for manual review
- **Idempotency**: Services handle duplicate events (Kafka at-least-once delivery)
- **Timeout**: All network calls have timeout configurations

### 7. Consensus (Kafka Internal)
- **KRaft Mode**: Kafka 3.x+ uses Raft for metadata consensus
  - Eliminates ZooKeeper dependency (we still use ZK for simplicity in POC)
  - Leader election for topic partitions
- **Production**: Migrate to KRaft mode for simpler operations

## Data Flow Example

### Happy Path: New Order
```
1. POST /orders → Order Service
2. Order Service:
   - Validates order
   - INSERT into orders_db (PostgreSQL)
   - COMMIT transaction
   - Publish "order.created" event to Kafka
3. Kafka:
   - Receives event
   - Writes to partition (determined by hash(order_id))
   - Acknowledges to producer
4. Inventory Service (Consumer Group: inventory-group):
   - Polls Kafka for new events
   - Receives "order.created"
   - Checks inventory in inventory_db
   - If available: Reserves stock, UPDATE inventory_db
   - COMMIT transaction
   - If unavailable: Publish "inventory.unavailable" event
5. Notification Service (Consumer Group: notification-group):
   - Polls Kafka for new events
   - Receives "order.created"
   - GET user preferences from Redis (cache)
   - If cache miss: Query from database, SET in Redis with TTL
   - Send notification (log simulation)
6. Search Service (Consumer Group: search-group):
   - Polls Kafka for new events
   - Receives "order.created"
   - Index order document in Elasticsearch
   - Document immediately searchable
```

### Failure Scenario: Inventory Unavailable
```
1. Order Service publishes "order.created"
2. Inventory Service:
   - Checks stock
   - Insufficient inventory
   - Publishes "inventory.unavailable" event
3. Order Service (listening to inventory events):
   - Consumes "inventory.unavailable"
   - UPDATE order status to "CANCELLED"
   - Publishes "order.cancelled" event
4. Notification Service:
   - Consumes "order.cancelled"
   - Notifies customer of cancellation
5. Search Service:
   - Consumes "order.cancelled"
   - Updates order status in Elasticsearch index
```

## Technology Stack Details

### PostgreSQL (orders_db, inventory_db)
- **Use Case**: ACID-compliant transactional data
- **Pattern**: Database per service (microservices principle)
- **Port**: 5432 (orders), 5433 (inventory)
- **Schema**: 
  - orders: id, customer_id, status, items, created_at, updated_at
  - inventory: product_id, quantity, reserved, available

### Redis
- **Use Case**: Caching user preferences, session data
- **Pattern**: Cache-aside (lazy loading)
- **Port**: 6379
- **Data Structures**: Strings (JSON), Hashes, Sets
- **TTL**: 1 hour for user preferences

### Kafka
- **Use Case**: Event streaming, asynchronous communication
- **Pattern**: Publish-Subscribe, Event Sourcing
- **Port**: 9092 (external), 29092 (inter-service)
- **Topics**: 
  - order.created (partitions: 3)
  - order.confirmed (partitions: 3)
  - order.cancelled (partitions: 3)
  - inventory.unavailable (partitions: 1)

### Elasticsearch
- **Use Case**: Full-text search, analytics
- **Pattern**: CQRS (Command Query Responsibility Segregation)
- **Port**: 9200 (HTTP), 9300 (Transport)
- **Index**: orders
- **Mappings**: Full-text on customer name, items; keyword on order_id, status

## Development URLs

- **Order Service API**: http://localhost:8001/docs (Swagger UI)
- **Kafka UI**: http://localhost:8080 (manage topics, view messages)
- **Kibana**: http://localhost:5601 (Elasticsearch visualization)
- **Elasticsearch**: http://localhost:9200 (query API)

## Testing Commands

### Check Redis Cache
```bash
docker exec -it redis redis-cli
GET user:preferences:cust-001
```

### View Kafka Messages
```bash
# Via UI: http://localhost:8080
# Or via CLI:
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order.created \
  --from-beginning
```

### View Postgres Tables
```bash
# via CLI:

# Orders: 5432 (psql)
docker exec -it postgres-orders psql -U orderuser -d orders_db

# Inventory: 5433 (psql)
docker exec -it postgres-inventory psql -U inventoryuser -d inventory_db`
```

### Documentation
- Kafka documentation: https://kafka.apache.org/documentation/
- Elasticsearch guide: https://www.elastic.co/guide/
- Redis documentation: https://redis.io/docs/
- PostgreSQL docs: https://www.postgresql.org/docs/