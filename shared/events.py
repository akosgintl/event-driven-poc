"""
Shared event models for event-driven communication via Kafka.

These models ensure consistency across all microservices.
All events follow CloudEvents specification for interoperability.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Dict, Any, List
import json
from enum import Enum


class EventType(Enum):
    """All event types in the system"""
    ORDER_CREATED = "order.created"
    ORDER_CONFIRMED = "order.confirmed"
    ORDER_CANCELLED = "order.cancelled"
    INVENTORY_RESERVED = "inventory.reserved"
    INVENTORY_UNAVAILABLE = "inventory.unavailable"
    INVENTORY_RELEASED = "inventory.released"


class OrderStatus(Enum):
    """Order status values"""
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    CANCELLED = "CANCELLED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"


@dataclass
class OrderItem:
    """Single item in an order"""
    product_id: str
    product_name: str
    quantity: int
    price: float

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class BaseEvent:
    """
    Base event following CloudEvents spec
    https://cloudevents.io/
    """
    event_id: str          # Unique event ID (UUID)
    event_type: str        # Event type (e.g., "order.created")
    event_time: str        # ISO 8601 timestamp
    source: str            # Service that produced the event
    data: Dict[str, Any]   # Event-specific payload
    trace_id: Optional[str] = None  # For distributed tracing

    def to_json(self) -> str:
        """Serialize to JSON for Kafka"""
        return json.dumps(asdict(self), default=str)

    @classmethod
    def from_json(cls, json_str: str) -> 'BaseEvent':
        """Deserialize from JSON"""
        data = json.loads(json_str)
        return cls(**data)


@dataclass
class OrderCreatedEvent(BaseEvent):
    """
    Published when a new order is created
    Consumer: Inventory, Notification, Search services
    """
    
    @classmethod
    def create(cls, order_id: str, customer_id: str, items: List[OrderItem], 
               total_amount: float, source: str = "order-service") -> 'OrderCreatedEvent':
        return cls(
            event_id=f"order-{order_id}-created",
            event_type=EventType.ORDER_CREATED.value,
            event_time=datetime.utcnow().isoformat(),
            source=source,
            data={
                "order_id": order_id,
                "customer_id": customer_id,
                "items": [item.to_dict() for item in items],
                "total_amount": total_amount,
                "status": OrderStatus.PENDING.value
            }
        )


@dataclass
class OrderConfirmedEvent(BaseEvent):
    """
    Published when order payment is confirmed
    Consumer: Notification, Search services
    """
    
    @classmethod
    def create(cls, order_id: str, customer_id: str, 
               source: str = "order-service") -> 'OrderConfirmedEvent':
        return cls(
            event_id=f"order-{order_id}-confirmed",
            event_type=EventType.ORDER_CONFIRMED.value,
            event_time=datetime.utcnow().isoformat(),
            source=source,
            data={
                "order_id": order_id,
                "customer_id": customer_id,
                "status": OrderStatus.CONFIRMED.value
            }
        )


@dataclass
class OrderCancelledEvent(BaseEvent):
    """
    Published when order is cancelled
    Consumer: Inventory (release stock), Notification, Search services
    """
    
    @classmethod
    def create(cls, order_id: str, customer_id: str, reason: str,
               source: str = "order-service") -> 'OrderCancelledEvent':
        return cls(
            event_id=f"order-{order_id}-cancelled",
            event_type=EventType.ORDER_CANCELLED.value,
            event_time=datetime.utcnow().isoformat(),
            source=source,
            data={
                "order_id": order_id,
                "customer_id": customer_id,
                "reason": reason,
                "status": OrderStatus.CANCELLED.value
            }
        )


@dataclass
class InventoryReservedEvent(BaseEvent):
    """
    Published when inventory is successfully reserved
    Consumer: Order service (to confirm order)
    """
    
    @classmethod
    def create(cls, order_id: str, items: List[Dict[str, Any]],
               source: str = "inventory-service") -> 'InventoryReservedEvent':
        return cls(
            event_id=f"inventory-{order_id}-reserved",
            event_type=EventType.INVENTORY_RESERVED.value,
            event_time=datetime.utcnow().isoformat(),
            source=source,
            data={
                "order_id": order_id,
                "items": items,
                "reserved": True
            }
        )


@dataclass
class InventoryUnavailableEvent(BaseEvent):
    """
    Published when inventory cannot be reserved (out of stock)
    Consumer: Order service (to cancel order)
    """
    
    @classmethod
    def create(cls, order_id: str, unavailable_items: List[Dict[str, Any]],
               source: str = "inventory-service") -> 'InventoryUnavailableEvent':
        return cls(
            event_id=f"inventory-{order_id}-unavailable",
            event_type=EventType.INVENTORY_UNAVAILABLE.value,
            event_time=datetime.utcnow().isoformat(),
            source=source,
            data={
                "order_id": order_id,
                "unavailable_items": unavailable_items,
                "reserved": False
            }
        )


# Topic configuration
KAFKA_TOPICS = {
    EventType.ORDER_CREATED.value: {
        "name": "order.created",
        "partitions": 3,
        "replication_factor": 1,  # Increase to 3 in production
        "config": {
            "retention.ms": 604800000,  # 7 days
            "cleanup.policy": "delete"
        }
    },
    EventType.ORDER_CONFIRMED.value: {
        "name": "order.confirmed",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000,
            "cleanup.policy": "delete"
        }
    },
    EventType.ORDER_CANCELLED.value: {
        "name": "order.cancelled",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000,
            "cleanup.policy": "delete"
        }
    },
    EventType.INVENTORY_RESERVED.value: {
        "name": "inventory.reserved",
        "partitions": 3,
        "replication_factor": 1,
        "config": {
            "retention.ms": 604800000,
            "cleanup.policy": "delete"
        }
    },
    EventType.INVENTORY_UNAVAILABLE.value: {
        "name": "inventory.unavailable",
        "partitions": 1,
        "replication_factor": 1,
        "config": {
            "retention.ms": 86400000,  # 1 day (shorter retention for errors)
            "cleanup.policy": "delete"
        }
    }
}


def get_partition_key(order_id: str) -> str:
    """
    Returns partition key for Kafka.
    Using order_id ensures all events for same order go to same partition,
    maintaining ordering guarantees.
    """
    return order_id
