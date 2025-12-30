FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY ./notification-service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY ./notification-service/main.py .

# Copy shared events module
COPY ./shared /app/shared

# Run the application
CMD ["python", "main.py"]
