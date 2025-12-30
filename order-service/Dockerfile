FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./order-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./order-service/main.py .

# Copy shared events module
COPY ./shared /app/shared

# Expose port
EXPOSE 8001

# Run the application
CMD ["python", "main.py"]
