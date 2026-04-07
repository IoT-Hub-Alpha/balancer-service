# Use official Python image
FROM python:3.13-slim

# Set working directory
WORKDIR /app
# Prevent Python from writing .pyc files & enable logs
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system dependencies (optional but useful)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY ./requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt



# Copy project
COPY app/ app/
COPY tests/ tests/
COPY pytest.ini pytest.ini

WORKDIR /app

EXPOSE 8055

# Run Django dev server on 0.0.0.0:8003
CMD ["python", "app/main.py"]