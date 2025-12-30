"""
Order Service - Receives orders, stores in PostgreSQL, publishes events to Kafka

Tech Stack:
- FastAPI: REST API framework
- PostgreSQL: Persistent storage
- Kafka: Event publishing
- SQLAlchemy: ORM
"""

from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, String, Float, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from kafka import KafkaProducer
from datetime import datetime
from typing import List, Optional
import json
import uuid
import logging

# Import shared events
from shared.events import (
    OrderCreatedEvent, OrderConfirmedEvent, OrderCancelledEvent,
    OrderItem, OrderStatus, get_partition_key
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database setup
DATABASE_URL = "postgresql://orderuser:orderpass@postgres-orders:5432/orders_db"
engine = create_engine(DATABASE_URL, echo=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Kafka setup
KAFKA_BOOTSTRAP_SERVERS = ['kafka:29092']
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: v.encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
    acks='all',  # Wait for all replicas to acknowledge (strongest durability)
    retries=3
)

# FastAPI app
app = FastAPI(title="Order Service", version="1.0.0")


# Database Models
class Order(Base):
    __tablename__ = "orders"

    id = Column(String, primary_key=True, index=True)
    customer_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, default=OrderStatus.PENDING.value)
    items = Column(Text, nullable=False)  # JSON string
    total_amount = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "customer_id": self.customer_id,
            "status": self.status,
            "items": json.loads(self.items),
            "total_amount": self.total_amount,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


# Pydantic models for API
class OrderItemRequest(BaseModel):
    product_id: str
    product_name: str
    quantity: int = Field(gt=0)
    price: float = Field(gt=0)


class CreateOrderRequest(BaseModel):
    customer_id: str
    items: List[OrderItemRequest]


class OrderResponse(BaseModel):
    id: str
    customer_id: str
    status: str
    items: List[dict]
    total_amount: float
    created_at: str
    updated_at: str


# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Initialize database
@app.on_event("startup")
async def startup_event():
    """Create tables on startup"""
    Base.metadata.create_all(bind=engine)
    logger.info("Order Service started successfully")


# Helper function to publish events
def publish_event(topic: str, event: str, partition_key: str):
    """Publish event to Kafka with retry logic"""
    try:
        future = producer.send(
            topic,
            value=event,
            key=partition_key
        )
        # Block for confirmation (for this simple POC)
        record_metadata = future.get(timeout=10)
        logger.info(f"Event published to {topic}: partition={record_metadata.partition}, offset={record_metadata.offset}")
    except Exception as e:
        logger.error(f"Failed to publish event to {topic}: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish event")


# API Endpoints
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "order-service"}


@app.post("/orders", response_model=OrderResponse, status_code=201)
async def create_order(request: CreateOrderRequest, db: Session = Depends(get_db)):
    """
    Create a new order
    
    Flow:
    1. Validate request
    2. Calculate total amount
    3. Store order in PostgreSQL
    4. Publish order.created event to Kafka
    5. Return order details
    """
    try:
        # Generate order ID
        order_id = str(uuid.uuid4())
        
        # Convert items to OrderItem objects
        items = [
            OrderItem(
                product_id=item.product_id,
                product_name=item.product_name,
                quantity=item.quantity,
                price=item.price
            )
            for item in request.items
        ]
        
        # Calculate total amount
        total_amount = sum(item.quantity * item.price for item in items)
        
        # Store order in database
        order = Order(
            id=order_id,
            customer_id=request.customer_id,
            status=OrderStatus.PENDING.value,
            items=json.dumps([item.to_dict() for item in items]),
            total_amount=total_amount
        )
        db.add(order)
        db.commit()
        db.refresh(order)
        
        logger.info(f"Order {order_id} created in database")
        
        # Publish event to Kafka
        event = OrderCreatedEvent.create(
            order_id=order_id,
            customer_id=request.customer_id,
            items=items,
            total_amount=total_amount
        )
        
        publish_event(
            topic="order.created",
            event=event.to_json(),
            partition_key=get_partition_key(order_id)
        )
        
        logger.info(f"Order {order_id} event published to Kafka")
        
        return order.to_dict()
        
    except Exception as e:
        logger.error(f"Error creating order: {e}")
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/orders/{order_id}", response_model=OrderResponse)
async def get_order(order_id: str, db: Session = Depends(get_db)):
    """Get order by ID"""
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    return order.to_dict()


@app.get("/orders")
async def list_orders(
    customer_id: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """List orders with optional filters"""
    query = db.query(Order)
    
    if customer_id:
        query = query.filter(Order.customer_id == customer_id)
    if status:
        query = query.filter(Order.status == status)
    
    orders = query.order_by(Order.created_at.desc()).limit(limit).all()
    return {"orders": [order.to_dict() for order in orders]}


@app.post("/orders/{order_id}/confirm")
async def confirm_order(order_id: str, db: Session = Depends(get_db)):
    """
    Confirm order (after inventory reservation)
    
    Publishes order.confirmed event
    """
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if order.status != OrderStatus.PENDING.value:
        raise HTTPException(status_code=400, detail=f"Cannot confirm order in {order.status} status")
    
    # Update status
    order.status = OrderStatus.CONFIRMED.value
    order.updated_at = datetime.utcnow()
    db.commit()
    
    # Publish event
    event = OrderConfirmedEvent.create(
        order_id=order_id,
        customer_id=order.customer_id
    )
    
    publish_event(
        topic="order.confirmed",
        event=event.to_json(),
        partition_key=get_partition_key(order_id)
    )
    
    logger.info(f"Order {order_id} confirmed")
    return order.to_dict()


@app.post("/orders/{order_id}/cancel")
async def cancel_order(order_id: str, reason: str, db: Session = Depends(get_db)):
    """
    Cancel order
    
    Publishes order.cancelled event
    """
    order = db.query(Order).filter(Order.id == order_id).first()
    if not order:
        raise HTTPException(status_code=404, detail="Order not found")
    
    if order.status in [OrderStatus.CANCELLED.value, OrderStatus.SHIPPED.value, OrderStatus.DELIVERED.value]:
        raise HTTPException(status_code=400, detail=f"Cannot cancel order in {order.status} status")
    
    # Update status
    order.status = OrderStatus.CANCELLED.value
    order.updated_at = datetime.utcnow()
    db.commit()
    
    # Publish event
    event = OrderCancelledEvent.create(
        order_id=order_id,
        customer_id=order.customer_id,
        reason=reason
    )
    
    publish_event(
        topic="order.cancelled",
        event=event.to_json(),
        partition_key=get_partition_key(order_id)
    )
    
    logger.info(f"Order {order_id} cancelled: {reason}")
    return order.to_dict()


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    producer.close()
    logger.info("Order Service shutdown")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
