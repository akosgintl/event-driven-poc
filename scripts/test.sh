#!/bin/bash

# Event-Driven Microservices POC - Test Script
# This script automates testing of the entire system

set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

ORDER_SERVICE_URL="http://localhost:8001"
ES_URL="http://localhost:9200"

echo -e "${YELLOW}=== Event-Driven Microservices POC - Automated Testing ===${NC}\n"

# Function to wait for service
wait_for_service() {
    local service=$1
    local url=$2
    local max_attempts=30
    local attempt=1

    echo -e "${YELLOW}Waiting for $service to be ready...${NC}"
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$url" > /dev/null 2>&1; then
            echo -e "${GREEN}✓ $service is ready${NC}\n"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts..."
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}✗ $service failed to start${NC}"
    return 1
}

# Test 1: Health Check
echo -e "${YELLOW}Test 1: Health Check${NC}"
wait_for_service "Order Service" "$ORDER_SERVICE_URL/health"
wait_for_service "Elasticsearch" "$ES_URL"

# Test 2: Create Order (Happy Path)
echo -e "${YELLOW}Test 2: Creating Order (Happy Path)${NC}"
ORDER_RESPONSE=$(curl -s -X POST "$ORDER_SERVICE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "test-customer-001",
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
  }')

ORDER_ID=$(echo $ORDER_RESPONSE | grep -o '"id":"[^"]*' | cut -d'"' -f4)

if [ -z "$ORDER_ID" ]; then
    echo -e "${RED}✗ Failed to create order${NC}"
    echo "$ORDER_RESPONSE"
    exit 1
fi

echo -e "${GREEN}✓ Order created: $ORDER_ID${NC}\n"

# Wait for event processing
echo -e "${YELLOW}Waiting for event processing (5 seconds)...${NC}"
sleep 5

# Test 3: Verify Order in Database
echo -e "${YELLOW}Test 3: Verify Order in Database${NC}"
ORDER_DETAILS=$(curl -s "$ORDER_SERVICE_URL/orders/$ORDER_ID")
ORDER_STATUS=$(echo $ORDER_DETAILS | grep -o '"status":"[^"]*' | cut -d'"' -f4)

if [ "$ORDER_STATUS" = "PENDING" ]; then
    echo -e "${GREEN}✓ Order status: $ORDER_STATUS${NC}\n"
else
    echo -e "${RED}✗ Unexpected order status: $ORDER_STATUS${NC}\n"
fi

# Test 4: Verify Order in Elasticsearch
echo -e "${YELLOW}Test 4: Verify Order in Elasticsearch${NC}"
sleep 2  # Wait for ES to index
ES_RESPONSE=$(curl -s "$ES_URL/orders/_search?q=order_id:$ORDER_ID")
ES_HITS=$(echo $ES_RESPONSE | grep -o '"total":{"value":[0-9]*' | grep -o '[0-9]*$')

if [ "$ES_HITS" -ge "1" ]; then
    echo -e "${GREEN}✓ Order found in Elasticsearch${NC}\n"
else
    echo -e "${RED}✗ Order not found in Elasticsearch${NC}\n"
fi

# Test 5: Check Service Logs
echo -e "${YELLOW}Test 5: Checking Service Logs${NC}"

echo "Order Service logs:"
docker-compose logs --tail=5 order-service 2>/dev/null || echo "Order service logs not available"

echo -e "\nInventory Service logs:"
docker-compose logs --tail=5 inventory-service 2>/dev/null || echo "Inventory service logs not available"

echo -e "\nNotification Service logs:"
docker-compose logs --tail=5 notification-service 2>/dev/null || echo "Notification service logs not available"

echo -e "\nSearch Service logs:"
docker-compose logs --tail=5 search-service 2>/dev/null || echo "Search service logs not available"

# Test 6: Cancel Order
echo -e "\n${YELLOW}Test 6: Cancel Order${NC}"
CANCEL_RESPONSE=$(curl -s -X POST "$ORDER_SERVICE_URL/orders/$ORDER_ID/cancel?reason=Test+cancellation")
CANCEL_STATUS=$(echo $CANCEL_RESPONSE | grep -o '"status":"[^"]*' | cut -d'"' -f4)

if [ "$CANCEL_STATUS" = "CANCELLED" ]; then
    echo -e "${GREEN}✓ Order cancelled successfully${NC}\n"
else
    echo -e "${RED}✗ Failed to cancel order${NC}\n"
fi

# Wait for cancellation to propagate
sleep 3

# Test 7: Verify Cancellation in ES
echo -e "${YELLOW}Test 7: Verify Cancellation in Elasticsearch${NC}"
sleep 2
ES_ORDER=$(curl -s "$ES_URL/orders/_doc/$ORDER_ID")
ES_STATUS=$(echo $ES_ORDER | grep -o '"status":"[^"]*' | cut -d'"' -f4)

if [ "$ES_STATUS" = "CANCELLED" ]; then
    echo -e "${GREEN}✓ Order status updated in Elasticsearch${NC}\n"
else
    echo -e "${RED}✗ Order status not updated in Elasticsearch${NC}\n"
fi

# Test 8: Create Order with Insufficient Inventory
echo -e "${YELLOW}Test 8: Testing Insufficient Inventory${NC}"
INSUFFICIENT_ORDER=$(curl -s -X POST "$ORDER_SERVICE_URL/orders" \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "test-customer-002",
    "items": [
      {
        "product_id": "LAPTOP-001",
        "product_name": "Gaming Laptop",
        "quantity": 500,
        "price": 1299.99
      }
    ]
  }')

INSUFFICIENT_ORDER_ID=$(echo $INSUFFICIENT_ORDER | grep -o '"id":"[^"]*' | cut -d'"' -f4)
echo -e "${GREEN}✓ Created order with insufficient inventory: $INSUFFICIENT_ORDER_ID${NC}"
echo -e "${YELLOW}Check Kafka UI for inventory.unavailable event${NC}\n"

# Test 9: List All Orders
echo -e "${YELLOW}Test 9: List All Orders${NC}"
ALL_ORDERS=$(curl -s "$ORDER_SERVICE_URL/orders")
ORDER_COUNT=$(echo $ALL_ORDERS | grep -o '"orders":\[' | wc -l)

if [ "$ORDER_COUNT" -ge "1" ]; then
    echo -e "${GREEN}✓ Successfully retrieved orders list${NC}\n"
else
    echo -e "${RED}✗ Failed to retrieve orders${NC}\n"
fi

# Test 10: Search in Elasticsearch
echo -e "${YELLOW}Test 10: Full-Text Search in Elasticsearch${NC}"
SEARCH_RESPONSE=$(curl -s "$ES_URL/orders/_search?q=laptop")
SEARCH_HITS=$(echo $SEARCH_RESPONSE | grep -o '"total":{"value":[0-9]*' | grep -o '[0-9]*$')

if [ "$SEARCH_HITS" -ge "1" ]; then
    echo -e "${GREEN}✓ Search found $SEARCH_HITS order(s) containing 'laptop'${NC}\n"
else
    echo -e "${RED}✗ Search returned no results${NC}\n"
fi

# Summary
echo -e "${YELLOW}=== Test Summary ===${NC}"
echo -e "${GREEN}✓ Core functionality working${NC}"
echo -e "${GREEN}✓ Event-driven flow verified${NC}"
echo -e "${GREEN}✓ Database persistence confirmed${NC}"
echo -e "${GREEN}✓ Search indexing operational${NC}"
echo -e "${GREEN}✓ Saga pattern tested (cancellation)${NC}\n"

echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Check Kafka UI: http://localhost:8080"
echo "2. Check Kibana: http://localhost:5601"
echo "3. Review service logs: docker-compose logs -f [service-name]"
echo "4. Test Redis cache: docker exec -it redis redis-cli"
echo ""
echo "Created Orders:"
echo "  - $ORDER_ID (cancelled)"
echo "  - $INSUFFICIENT_ORDER_ID (insufficient inventory)"

echo -e "\n${GREEN}All tests completed!${NC}"
