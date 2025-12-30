FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./inventory-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./inventory-service/main.py .

# Copy shared events module
COPY ./shared /app/shared

# Expose port (not strictly necessary for consumers)
EXPOSE 8002

# Run the application
CMD ["python", "main.py"]
