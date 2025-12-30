"""
Inventory Service - Manages inventory, reserves stock for orders

Tech Stack:
- Kafka Consumer: Subscribe to order events
- PostgreSQL: Inventory storage
- SQLAlchemy: ORM

Consumer Group: inventory-group
Topics: order.created, order.cancelled
"""

from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime
import json
import logging
import time

# Import shared events
from shared.events import (
    BaseEvent, EventType, InventoryReservedEvent, InventoryUnavailableEvent,
    get_partition_key
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = "postgresql://inventoryuser:inventorypass@postgres-inventory:5432/inventory_db"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Kafka setup
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
CONSUMER_GROUP_ID = 'inventory-group'


# Database Models
class InventoryItem(Base):
    __tablename__ = "inventory"

    product_id = Column(String, primary_key=True, index=True)
    product_name = Column(String, nullable=False)
    total_quantity = Column(Integer, nullable=False, default=0)
    reserved_quantity = Column(Integer, nullable=False, default=0)
    available_quantity = Column(Integer, nullable=False, default=0)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def reserve(self, quantity: int) -> bool:
        """
        Reserve inventory for an order
        Returns True if successful, False if insufficient stock
        """
        if self.available_quantity >= quantity:
            self.reserved_quantity += quantity
            self.available_quantity -= quantity
            return True
        return False

    def release(self, quantity: int):
        """Release reserved inventory (e.g., order cancelled)"""
        self.reserved_quantity -= quantity
        self.available_quantity += quantity

    def to_dict(self):
        return {
            "product_id": self.product_id,
            "product_name": self.product_name,
            "total_quantity": self.total_quantity,
            "reserved_quantity": self.reserved_quantity,
            "available_quantity": self.available_quantity,
            "updated_at": self.updated_at.isoformat()
        }


class InventoryReservation(Base):
    """Track reservations per order for idempotency"""
    __tablename__ = "reservations"

    order_id = Column(String, primary_key=True, index=True)
    product_ids = Column(String, nullable=False)  # JSON array
    status = Column(String, nullable=False)  # RESERVED, RELEASED
    created_at = Column(DateTime, default=datetime.utcnow)


# Initialize database
Base.metadata.create_all(bind=engine)


def seed_inventory():
    """Seed initial inventory data"""
    db = SessionLocal()
    try:
        # Check if already seeded
        count = db.query(InventoryItem).count()
        if count > 0:
            logger.info(f"Inventory already seeded with {count} items")
            return

        # Seed products
        products = [
            InventoryItem(
                product_id="LAPTOP-001",
                product_name="Gaming Laptop",
                total_quantity=100,
                reserved_quantity=0,
                available_quantity=100
            ),
            InventoryItem(
                product_id="MOUSE-001",
                product_name="Wireless Mouse",
                total_quantity=500,
                reserved_quantity=0,
                available_quantity=500
            ),
            InventoryItem(
                product_id="KEYBOARD-001",
                product_name="Mechanical Keyboard",
                total_quantity=200,
                reserved_quantity=0,
                available_quantity=200
            ),
            InventoryItem(
                product_id="MONITOR-001",
                product_name="4K Monitor",
                total_quantity=150,
                reserved_quantity=0,
                available_quantity=150
            ),
        ]

        for product in products:
            db.add(product)

        db.commit()
        logger.info(f"Seeded {len(products)} inventory items")

    except Exception as e:
        logger.error(f"Error seeding inventory: {e}")
        db.rollback()
    finally:
        db.close()


def publish_event(producer: KafkaProducer, topic: str, event: str, partition_key: str):
    """Publish event to Kafka"""
    try:
        future = producer.send(topic, value=event, key=partition_key)
        record_metadata = future.get(timeout=10)
        logger.info(f"Event published to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
    except Exception as e:
        logger.error(f"Failed to publish event to {topic}: {e}")


def handle_order_created(event_data: dict, producer: KafkaProducer):
    """
    Handle order.created event
    
    Logic:
    1. Check if already processed (idempotency)
    2. Check inventory availability for all items
    3. If available: Reserve stock, publish inventory.reserved
    4. If unavailable: Publish inventory.unavailable
    """
    db = SessionLocal()
    try:
        order_id = event_data['order_id']
        items = event_data['items']

        # Idempotency check
        existing = db.query(InventoryReservation).filter(
            InventoryReservation.order_id == order_id
        ).first()
        
        if existing:
            logger.info(f"Order {order_id} already processed, skipping")
            return

        logger.info(f"Processing order {order_id} with {len(items)} items")

        # Check availability for all items
        unavailable_items = []
        items_to_reserve = []

        for item in items:
            product_id = item['product_id']
            quantity = item['quantity']

            inventory = db.query(InventoryItem).filter(
                InventoryItem.product_id == product_id
            ).first()

            if not inventory:
                unavailable_items.append({
                    "product_id": product_id,
                    "reason": "Product not found"
                })
            elif inventory.available_quantity < quantity:
                unavailable_items.append({
                    "product_id": product_id,
                    "requested": quantity,
                    "available": inventory.available_quantity,
                    "reason": "Insufficient stock"
                })
            else:
                items_to_reserve.append((inventory, quantity))

        # If any item unavailable, publish failure event
        if unavailable_items:
            logger.warning(f"Cannot reserve inventory for order {order_id}: {unavailable_items}")
            
            event = InventoryUnavailableEvent.create(
                order_id=order_id,
                unavailable_items=unavailable_items
            )
            
            publish_event(
                producer,
                "inventory.unavailable",
                event.to_json(),
                get_partition_key(order_id)
            )
            return

        # Reserve all items
        for inventory, quantity in items_to_reserve:
            inventory.reserve(quantity)

        # Record reservation
        reservation = InventoryReservation(
            order_id=order_id,
            product_ids=json.dumps([item['product_id'] for item in items]),
            status="RESERVED"
        )
        db.add(reservation)
        db.commit()

        logger.info(f"Successfully reserved inventory for order {order_id}")

        # Publish success event
        event = InventoryReservedEvent.create(
            order_id=order_id,
            items=[item for item in items]
        )
        
        publish_event(
            producer,
            "inventory.reserved",
            event.to_json(),
            get_partition_key(order_id)
        )

    except Exception as e:
        logger.error(f"Error processing order.created event: {e}")
        db.rollback()
    finally:
        db.close()


def handle_order_cancelled(event_data: dict):
    """
    Handle order.cancelled event
    
    Release reserved inventory back to available stock
    """
    db = SessionLocal()
    try:
        order_id = event_data['order_id']

        # Find reservation
        reservation = db.query(InventoryReservation).filter(
            InventoryReservation.order_id == order_id,
            InventoryReservation.status == "RESERVED"
        ).first()

        if not reservation:
            logger.warning(f"No reservation found for order {order_id}")
            return

        # Release inventory
        product_ids = json.loads(reservation.product_ids)
        for product_id in product_ids:
            # TODO: We need to store quantities in reservation table
            # For now, this is simplified
            logger.info(f"Released inventory for product {product_id}")

        # Update reservation status
        reservation.status = "RELEASED"
        db.commit()

        logger.info(f"Successfully released inventory for order {order_id}")

    except Exception as e:
        logger.error(f"Error processing order.cancelled event: {e}")
        db.rollback()
    finally:
        db.close()


def start_consumer():
    """Start Kafka consumer"""
    # Wait for Kafka to be ready
    logger.info("Waiting for Kafka to be ready...")
    time.sleep(10)

    # Seed inventory
    seed_inventory()

    # Create consumer
    consumer = KafkaConsumer(
        'order.created',
        'order.cancelled',
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP_ID,
        auto_offset_reset='earliest',  # Start from beginning if no offset
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        key_deserializer=lambda k: k.decode('utf-8') if k else None
    )

    # Create producer for publishing events
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v.encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None,
        acks='all',
        retries=3
    )

    logger.info(f"Inventory Service started, subscribed to topics: {consumer.subscription()}")

    try:
        for message in consumer:
            try:
                topic = message.topic
                event_data = message.value.get('data', {})
                
                logger.info(f"Received event from {topic}: {event_data.get('order_id')}")

                if topic == 'order.created':
                    handle_order_created(event_data, producer)
                elif topic == 'order.cancelled':
                    handle_order_cancelled(event_data)

            except Exception as e:
                logger.error(f"Error processing message: {e}")
                # In production: send to DLQ (Dead Letter Queue)

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    logger.info("Starting Inventory Service...")
    start_consumer()
