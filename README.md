# Event-Driven Microservices POC

A hands-on proof-of-concept demonstrating event-driven microservices architecture using **Redis**, **Kafka**, **Elasticsearch**, and **PostgreSQL**.

## 🎯 Learning Objectives

This POC demonstrates distributed systems concepts:
- **Event-driven architecture** (Saga pattern - choreography)
- **Database per service** (microservices pattern)
- **CAP theorem tradeoffs** (CP vs AP systems)
- **Partitioning strategies** (Kafka hash partitioning)
- **Replication patterns** (leader-follower)
- **Service discovery** (Docker DNS)
- **Fault tolerance** (idempotent consumers, retry logic)

## 📐 Architecture

```
Order Service (FastAPI + PostgreSQL)
     │
     ├─> Publishes: order.created, order.confirmed, order.cancelled
     │
     v
   Kafka (Event Bus)
     │
     ├─> Inventory Service (PostgreSQL)
     │   └─> Reserves/releases stock
     │
     ├─> Notification Service (Redis cache)
     │   └─> Sends customer notifications
     │
     └─> Search Service (Elasticsearch)
         └─> Indexes orders for search
```

## 🛠 Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Order Service** | FastAPI + PostgreSQL | Receive orders, publish events |
| **Inventory Service** | Python + PostgreSQL | Manage stock levels |
| **Notification Service** | Python + Redis | Send notifications with cached preferences |
| **Search Service** | Python + Elasticsearch | Index orders for full-text search |
| **Event Bus** | Kafka + Zookeeper | Asynchronous event streaming |
| **Monitoring** | Kafka UI + Kibana | Kafka and Elasticsearch visualization |

## 📋 Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- 8GB RAM minimum
- Ports available: 5432, 5433, 6379, 9092, 9200, 5601, 8080, 8001

## 🚀 Quick Start

### 1. Start Infrastructure Services

```bash
# Clone or navigate to project directory
cd event-driven-poc

# Start all services (will take 2-3 minutes first time)
docker-compose up -d

# Check service health
docker-compose ps

# View logs
docker-compose logs -f
```

### 2. Wait for Services to be Ready

```bash
# Check Kafka is ready
docker-compose logs kafka | grep "started (kafka.server.KafkaServer)"

# Check PostgreSQL is ready
docker-compose logs postgres-orders | grep "ready to accept connections"
docker-compose logs postgres-inventory | grep "ready to accept connections"

# Check Elasticsearch is ready
curl http://localhost:9200/_cluster/health
```

### 3. Access Management UIs

- **Kafka UI**: http://localhost:8080 (manage topics, view messages)
- **Kibana**: http://localhost:5601 (Elasticsearch visualization)
- **Elasticsearch API**: http://localhost:9200

## 🧪 Testing the System

### Test 1: Create an Order (Happy Path)

```bash
# Create a new order
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-123",
    "items": [
      {
        "product_id": "LAPTOP-001",
        "product_name": "Gaming Laptop",
        "quantity": 1,
        "price": 1299.99
      },
      {
        "product_id": "MOUSE-001",
        "product_name": "Wireless Mouse",
        "quantity": 2,
        "price": 29.99
      }
    ]
  }'

# Response will include order_id - save it for next steps
# Example: {"id":"a7f8e3d2-...","status":"PENDING",...}
```

### Test 2: Verify Event Flow

```bash
# 1. Check Kafka UI - http://localhost:8080
#    Navigate to Topics > order.created
#    You should see your order event

# 2. Check Inventory Service logs
docker-compose logs inventory-service | grep "Successfully reserved"

# 3. Check Notification Service logs
docker-compose logs notification-service | grep "NOTIFICATION"

# 4. Check Search Service logs
docker-compose logs search-service | grep "Indexed order"
```

### Test 3: Query Order

```bash
# Get order by ID
curl http://localhost:8001/orders/{order_id}

# List all orders
curl http://localhost:8001/orders

# Filter by customer
curl "http://localhost:8001/orders?customer_id=customer-123"
```

### Test 4: Search Orders in Elasticsearch

```bash
# Search for orders containing "laptop"
curl -X GET "http://localhost:9200/orders/_search?q=laptop&pretty"

# Get all orders
curl -X GET "http://localhost:9200/orders/_search?pretty"

# Search by customer ID
curl -X POST "http://localhost:9200/orders/_search?pretty" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "match": {
        "customer_id": "customer-123"
      }
    }
  }'
```

### Test 5: Check Redis Cache

```bash
# Connect to Redis
docker exec -it redis redis-cli

# Check cached user preferences
GET user:preferences:customer-123

# Check notification stats
GET stats:notifications:ORDER_CREATED

# Check customer notification history
LRANGE customer:notifications:customer-123 0 -1

# Exit
exit
```

### Test 6: Cancel Order (Saga Compensation)

```bash
# Cancel an order
curl -X POST "http://localhost:8001/orders/{order_id}/cancel?reason=Customer+requested" \
  -H "Content-Type: application/json"

# Verify:
# 1. Order status updated to CANCELLED
curl http://localhost:8001/orders/{order_id}

# 2. Inventory released (check logs)
docker-compose logs inventory-service | grep "Released inventory"

# 3. Cancellation notification sent
docker-compose logs notification-service | grep "ORDER_CANCELLED"

# 4. Elasticsearch index updated
curl -X GET "http://localhost:9200/orders/_doc/{order_id}?pretty"
```

### Test 7: Test Insufficient Inventory

```bash
# Try to order more than available stock
curl -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "customer-456",
    "items": [
      {
        "product_id": "LAPTOP-001",
        "product_name": "Gaming Laptop",
        "quantity": 200,
        "price": 1299.99
      }
    ]
  }'

# Check Kafka UI for "inventory.unavailable" event
# Order should remain in PENDING status

# Check logs
docker-compose logs inventory-service | grep "Cannot reserve inventory"
```

## 📊 Monitoring and Observability

### View Kafka Topics

```bash
# Via Kafka UI: http://localhost:8080
# Or via CLI:
docker exec -it kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --list
```

### View Kafka Messages

```bash
# Read messages from a topic
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic order.created \
  --from-beginning
```

### Check Database Content

```bash
# PostgreSQL - Orders
docker exec -it postgres-orders psql -U orderuser -d orders_db
# Then: SELECT * FROM orders;

# PostgreSQL - Inventory
docker exec -it postgres-inventory psql -U inventoryuser -d inventory_db
# Then: SELECT * FROM inventory;
```

### Elasticsearch Queries

```bash
# Get index statistics
curl "http://localhost:9200/orders/_stats?pretty"

# Get mapping
curl "http://localhost:9200/orders/_mapping?pretty"

# Get all documents
curl "http://localhost:9200/orders/_search?size=100&pretty"
```

## 🔍 Distributed Systems Concepts in Action

### 1. CAP Theorem

- **PostgreSQL (CP)**: Strong consistency for transactional data
  - Order creation is ACID-compliant
  - Inventory reservations use database transactions
  
- **Elasticsearch (AP)**: Eventual consistency for search
  - Search index might be slightly behind (refresh_interval: 1s)
  - Prioritizes availability and low latency

- **Redis (CP with Sentinel)**: Strong consistency for cache
  - Cache-aside pattern with TTL
  - Falls back to database on cache miss

### 2. Event-Driven Architecture (Saga Pattern)

- **Choreography**: Services react to events independently
- **No distributed transactions**: Each service has local transactions
- **Compensating actions**: Order cancellation triggers inventory release
- **Idempotency**: Services handle duplicate events (Kafka at-least-once)

### 3. Partitioning

- **Kafka**: Hash-based partitioning on `order_id`
  - All events for same order go to same partition
  - Maintains ordering guarantees per order
  - 3 partitions for parallelism

### 4. Consumer Groups

- Each service is a separate consumer group
- Multiple instances can share the load within a group
- Each event consumed by one instance per group

### 5. Fault Tolerance

- **Idempotent consumers**: Handle duplicate events gracefully
- **Database transactions**: Atomic operations within each service
- **Health checks**: Docker healthcheck probes
- **Retry logic**: Kafka consumer auto-retry on failure

## 🏗 Next Steps for Production

### Intermediate Phase
- [ ] Add API Gateway (Kong/Nginx)
- [ ] Implement circuit breakers (Resilience4j)
- [ ] Add distributed tracing (Jaeger/Zipkin)
- [ ] Dead letter queues for failed events
- [ ] Monitoring (Prometheus + Grafana)

### Advanced Phase
- [ ] Multi-node Kafka cluster (RF=3)
- [ ] PostgreSQL streaming replication
- [ ] Redis Cluster (partitioning)
- [ ] Elasticsearch cluster (3 nodes)
- [ ] Service mesh (Istio/Linkerd)
- [ ] Saga orchestration (Temporal/Camunda)

### Cloud Migration (Azure/AWS)
- [ ] Managed Kafka (Confluent Cloud/Event Hubs/MSK)
- [ ] Managed PostgreSQL
- [ ] Managed Redis
- [ ] Kubernetes deployment (AKS/EKS)
- [ ] Cloud-native monitoring

## 🐛 Troubleshooting

### Services not starting

```bash
# Check resource usage
docker stats

# Check logs for errors
docker-compose logs [service-name]

# Restart specific service
docker-compose restart [service-name]
```

### Kafka connection issues

```bash
# Check Kafka is running
docker-compose ps kafka

# Check Kafka logs
docker-compose logs kafka | tail -100

# Restart Kafka
docker-compose restart kafka zookeeper
```

### Database connection issues

```bash
# Check PostgreSQL logs
docker-compose logs postgres-orders

# Test connection
docker exec -it postgres-orders pg_isready -U orderuser
```

### Port conflicts

```bash
# Check which process is using a port
lsof -i :9092  # On Linux/Mac
netstat -ano | findstr :9092  # On Windows

# Change port in docker-compose.yml if needed
```

## 🧹 Cleanup

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (clean slate)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```
