"""
Notification Service - Sends notifications using cached user preferences

Tech Stack:
- Kafka Consumer: Subscribe to order events
- Redis: Cache user notification preferences
- (In real scenario: SendGrid/Twilio for actual notifications)

Consumer Group: notification-group
Topics: order.created, order.confirmed, order.cancelled
"""

from kafka import KafkaConsumer
import redis
import json
import logging
import time

# Import shared events
from shared.events import EventType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis setup
REDIS_HOST = 'redis'
REDIS_PORT = 6379
REDIS_DB = 0
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    decode_responses=True
)

# Kafka setup
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
CONSUMER_GROUP_ID = 'notification-group'

# Constants
PREFERENCE_TTL = 3600  # 1 hour cache TTL
DEFAULT_PREFERENCES = {
    "email_enabled": True,
    "sms_enabled": False,
    "language": "en"
}


def get_user_preferences(customer_id: str) -> dict:
    """
    Get user notification preferences from Redis
    
    Cache-aside pattern:
    1. Check Redis cache
    2. If miss, fetch from database (simulated here)
    3. Store in cache with TTL
    """
    cache_key = f"user:preferences:{customer_id}"
    
    # Try to get from cache
    cached = redis_client.get(cache_key)
    if cached:
        logger.info(f"Cache HIT for user {customer_id}")
        return json.loads(cached)
    
    # Cache miss - simulate database fetch
    logger.info(f"Cache MISS for user {customer_id}, fetching from database")
    
    # In real scenario: fetch from user database
    # For now, use defaults with some variation
    preferences = DEFAULT_PREFERENCES.copy()
    preferences["customer_id"] = customer_id
    
    # Store in cache
    redis_client.setex(
        cache_key,
        PREFERENCE_TTL,
        json.dumps(preferences)
    )
    
    return preferences


def send_notification(notification_type: str, customer_id: str, order_id: str, data: dict):
    """
    Send notification to customer
    
    In production: integrate with SendGrid, Twilio, Firebase, etc.
    For POC: log the notification
    """
    preferences = get_user_preferences(customer_id)
    
    notification = {
        "type": notification_type,
        "customer_id": customer_id,
        "order_id": order_id,
        "channels": [],
        "data": data,
        "timestamp": data.get('event_time', '')
    }
    
    # Determine channels based on preferences
    if preferences.get("email_enabled"):
        notification["channels"].append("email")
    if preferences.get("sms_enabled"):
        notification["channels"].append("sms")
    
    # Log notification (in production: actually send)
    logger.info(f"""
    ===== NOTIFICATION =====
    Type: {notification_type}
    Customer: {customer_id}
    Order: {order_id}
    Channels: {notification["channels"]}
    Language: {preferences.get('language', 'en')}
    Data: {json.dumps(data, indent=2)}
    ========================
    """)
    
    # Track notification in Redis (analytics)
    stats_key = f"stats:notifications:{notification_type}"
    redis_client.incr(stats_key)
    
    # Store recent notifications for customer (list with TTL)
    customer_notifs_key = f"customer:notifications:{customer_id}"
    redis_client.lpush(customer_notifs_key, json.dumps(notification))
    redis_client.ltrim(customer_notifs_key, 0, 99)  # Keep last 100
    redis_client.expire(customer_notifs_key, 86400)  # 24 hours


def handle_order_created(event_data: dict):
    """Handle order.created event"""
    customer_id = event_data['customer_id']
    order_id = event_data['order_id']
    items = event_data['items']
    total_amount = event_data['total_amount']
    
    send_notification(
        notification_type="ORDER_CREATED",
        customer_id=customer_id,
        order_id=order_id,
        data={
            "message": "Your order has been received!",
            "items_count": len(items),
            "total_amount": total_amount,
            "event_time": event_data.get('event_time')
        }
    )


def handle_order_confirmed(event_data: dict):
    """Handle order.confirmed event"""
    customer_id = event_data['customer_id']
    order_id = event_data['order_id']
    
    send_notification(
        notification_type="ORDER_CONFIRMED",
        customer_id=customer_id,
        order_id=order_id,
        data={
            "message": "Your order has been confirmed and will be shipped soon!",
            "event_time": event_data.get('event_time')
        }
    )


def handle_order_cancelled(event_data: dict):
    """Handle order.cancelled event"""
    customer_id = event_data['customer_id']
    order_id = event_data['order_id']
    reason = event_data.get('reason', 'Unknown')
    
    send_notification(
        notification_type="ORDER_CANCELLED",
        customer_id=customer_id,
        order_id=order_id,
        data={
            "message": "Your order has been cancelled.",
            "reason": reason,
            "event_time": event_data.get('event_time')
        }
    )


def start_consumer():
    """Start Kafka consumer"""
    # Wait for services to be ready
    logger.info("Waiting for Kafka and Redis to be ready...")
    time.sleep(10)
    
    # Test Redis connection
    try:
        redis_client.ping()
        logger.info("Redis connection successful")
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        return
    
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
    
    logger.info(f"Notification Service started, subscribed to topics: {consumer.subscription()}")
    
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
    logger.info("Starting Notification Service...")
    start_consumer()
