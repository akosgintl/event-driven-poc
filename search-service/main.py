"""
Search Service - Indexes orders in Elasticsearch for full-text search

Tech Stack:
- Kafka Consumer: Subscribe to order events
- Elasticsearch: Search and analytics

Consumer Group: search-group
Topics: order.created, order.confirmed, order.cancelled
"""

from kafka import KafkaConsumer
from elasticsearch import Elasticsearch, helpers
import json
import logging
import time

# Import shared events
from shared.events import EventType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Elasticsearch setup
ES_HOST = 'elasticsearch'
ES_PORT = 9200
es_client = Elasticsearch([f'http://{ES_HOST}:{ES_PORT}'])

# Kafka setup
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
CONSUMER_GROUP_ID = 'search-group'

# Index name
INDEX_NAME = 'orders'

# Elasticsearch index mapping
INDEX_MAPPING = {
    "mappings": {
        "properties": {
            "order_id": {"type": "keyword"},
            "customer_id": {"type": "keyword"},
            "status": {"type": "keyword"},
            "items": {
                "type": "nested",
                "properties": {
                    "product_id": {"type": "keyword"},
                    "product_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "quantity": {"type": "integer"},
                    "price": {"type": "float"}
                }
            },
            "total_amount": {"type": "float"},
            "event_time": {"type": "date"},
            "indexed_at": {"type": "date"},
            # For full-text search
            "search_text": {
                "type": "text",
                "analyzer": "standard"
            }
        }
    },
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0,  # No replicas in single-node setup
        "refresh_interval": "1s"
    }
}


def create_index():
    """Create Elasticsearch index if it doesn't exist"""
    try:
        if not es_client.indices.exists(index=INDEX_NAME):
            es_client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
            logger.info(f"Created index: {INDEX_NAME}")
        else:
            logger.info(f"Index {INDEX_NAME} already exists")
    except Exception as e:
        logger.error(f"Error creating index: {e}")


def index_order(order_data: dict):
    """
    Index order document in Elasticsearch
    
    Implements CQRS pattern: Write model (PostgreSQL) separate from Read model (Elasticsearch)
    """
    try:
        order_id = order_data['order_id']
        
        # Build search text for full-text search
        search_text_parts = [
            order_data['order_id'],
            order_data['customer_id']
        ]
        
        # Add product names to search text
        for item in order_data.get('items', []):
            search_text_parts.append(item.get('product_name', ''))
        
        document = {
            "order_id": order_data['order_id'],
            "customer_id": order_data['customer_id'],
            "status": order_data['status'],
            "items": order_data.get('items', []),
            "total_amount": order_data.get('total_amount', 0),
            "event_time": order_data.get('event_time'),
            "indexed_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
            "search_text": " ".join(search_text_parts)
        }
        
        # Index or update document
        es_client.index(
            index=INDEX_NAME,
            id=order_id,  # Use order_id as document ID for idempotency
            body=document
        )
        
        logger.info(f"Indexed order {order_id} in Elasticsearch")
        
    except Exception as e:
        logger.error(f"Error indexing order: {e}")


def update_order_status(order_id: str, status: str):
    """Update order status in Elasticsearch"""
    try:
        es_client.update(
            index=INDEX_NAME,
            id=order_id,
            body={
                "doc": {
                    "status": status,
                    "indexed_at": time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                }
            }
        )
        logger.info(f"Updated order {order_id} status to {status}")
    except Exception as e:
        logger.error(f"Error updating order status: {e}")


def handle_order_created(event_data: dict):
    """Handle order.created event - Index new order"""
    index_order(event_data)


def handle_order_confirmed(event_data: dict):
    """Handle order.confirmed event - Update status"""
    order_id = event_data['order_id']
    update_order_status(order_id, event_data['status'])


def handle_order_cancelled(event_data: dict):
    """Handle order.cancelled event - Update status"""
    order_id = event_data['order_id']
    update_order_status(order_id, event_data['status'])


def search_orders(query: str, limit: int = 10):
    """
    Search orders using full-text search
    
    Example usage (can be exposed via REST API):
    - Search by order ID: "ORDER-123"
    - Search by customer: "customer-456"
    - Search by product: "laptop"
    """
    try:
        body = {
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["search_text", "order_id^2", "customer_id^2"],
                    "type": "best_fields"
                }
            },
            "size": limit,
            "sort": [{"event_time": {"order": "desc"}}]
        }
        
        response = es_client.search(index=INDEX_NAME, body=body)
        hits = response['hits']['hits']
        
        results = [hit['_source'] for hit in hits]
        logger.info(f"Search '{query}' returned {len(results)} results")
        
        return results
        
    except Exception as e:
        logger.error(f"Error searching orders: {e}")
        return []


def get_order_stats():
    """Get aggregated order statistics"""
    try:
        body = {
            "size": 0,
            "aggs": {
                "status_breakdown": {
                    "terms": {"field": "status"}
                },
                "total_revenue": {
                    "sum": {"field": "total_amount"}
                },
                "avg_order_value": {
                    "avg": {"field": "total_amount"}
                },
                "orders_over_time": {
                    "date_histogram": {
                        "field": "event_time",
                        "calendar_interval": "day"
                    }
                }
            }
        }
        
        response = es_client.search(index=INDEX_NAME, body=body)
        return response['aggregations']
        
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        return {}


def start_consumer():
    """Start Kafka consumer"""
    # Wait for services to be ready
    logger.info("Waiting for Kafka and Elasticsearch to be ready...")
    time.sleep(15)  # Elasticsearch takes longer to start
    
    # Test Elasticsearch connection
    max_retries = 10
    for i in range(max_retries):
        try:
            if es_client.ping():
                logger.info("Elasticsearch connection successful")
                break
        except Exception as e:
            logger.warning(f"Elasticsearch not ready (attempt {i+1}/{max_retries}): {e}")
            time.sleep(5)
    else:
        logger.error("Could not connect to Elasticsearch")
        return
    
    # Create index
    create_index()
    
    # Create consumer
    consumer = KafkaConsumer(
        'order.created',
        'order.confirmed',
        'order.cancelled',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )
    
    logger.info(f"Search Service started, subscribed to topics: {consumer.subscription()}")
    
    try:
        for message in consumer:
            try:
                topic = message.topic
                event = message.value
                event_data = event.get('data', {})
                
                logger.info(f"Received event from {topic}: {event_data.get('order_id')}")
                
                if topic == 'order.created':
                    handle_order_created(event_data)
                elif topic == 'order.confirmed':
                    handle_order_confirmed(event_data)
                elif topic == 'order.cancelled':
                    handle_order_cancelled(event_data)
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # In production: send to DLQ
                
    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    logger.info("Starting Search Service...")
    start_consumer()
