# Use an official Python image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Copy project files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY data/ ./data/
COPY outputs/ ./outputs/
COPY main.py .

# Run the reconciliation engine
CMD ["python", "main.py"]