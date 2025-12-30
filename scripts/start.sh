#!/bin/bash

# Quick Start Script for Event-Driven Microservices POC

set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}"
cat << "EOF"
╔═══════════════════════════════════════════════════════════╗
║   Event-Driven Microservices POC                         ║
║   Redis + Kafka + Elasticsearch + PostgreSQL             ║
╚═══════════════════════════════════════════════════════════╝
EOF
echo -e "${NC}"

# Check prerequisites
echo -e "${YELLOW}Checking prerequisites...${NC}"

if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker and Docker Compose are installed${NC}\n"

# Check if services are already running
if docker-compose ps | grep -q "Up"; then
    echo -e "${YELLOW}Services are already running!${NC}"
    echo "To restart: ./start.sh restart"
    echo "To stop: docker-compose down"
    echo ""
    docker-compose ps
    exit 0
fi

# Start services
echo -e "${YELLOW}Starting all services...${NC}"
echo "This will take 2-3 minutes on first run (downloading images)"
echo ""

docker-compose up -d

echo ""
echo -e "${YELLOW}Waiting for services to be healthy...${NC}"

# Wait function
wait_for_service() {
    local service=$1
    local max_attempts=30
    local attempt=1

    while [ $attempt -le $max_attempts ]; do
        if docker-compose ps | grep "$service" | grep -q "healthy\|Up"; then
            echo -e "${GREEN}✓ $service is ready${NC}"
            return 0
        fi
        echo "Waiting for $service... ($attempt/$max_attempts)"
        sleep 2
        ((attempt++))
    done
    
    echo -e "${RED}✗ $service failed to start${NC}"
    return 1
}

# Check critical services
wait_for_service "postgres-orders"
wait_for_service "postgres-inventory"
wait_for_service "redis"
wait_for_service "kafka"
wait_for_service "elasticsearch"

echo ""
echo -e "${GREEN}All infrastructure services are ready!${NC}"
echo ""

# Wait for microservices
echo -e "${YELLOW}Starting microservices...${NC}"
sleep 5

echo ""
echo -e "${GREEN}╔═══════════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              System is ready for testing!                ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${BLUE}Access Points:${NC}"
echo "  • Order Service API:  http://localhost:8001/docs"
echo "  • Kafka UI:           http://localhost:8080"
echo "  • Kibana:             http://localhost:5601"
echo "  • Elasticsearch:      http://localhost:9200"
echo ""

echo -e "${BLUE}Quick Commands:${NC}"
echo "  • View logs:          docker-compose logs -f [service-name]"
echo "  • Check status:       docker-compose ps"
echo "  • Run tests:          ./test.sh"
echo "  • Stop services:      docker-compose down"
echo "  • Clean reset:        docker-compose down -v"
echo ""

echo -e "${YELLOW}Next Steps:${NC}"
echo "1. Run automated tests:  ${GREEN}./test.sh${NC}"
echo "2. Read the README:      ${GREEN}cat README.md${NC}"
echo "3. Try manual testing:   ${GREEN}curl http://localhost:8001/health${NC}"
echo ""

# Ask if user wants to run tests
read -p "Run automated tests now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo ""
    echo -e "${YELLOW}Running automated tests...${NC}"
    echo ""
    ./test.sh
fi
