#!/bin/bash

echo "🚀 Deploying Data Warehouse with Gold DW..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Stop any existing containers
echo "📦 Stopping existing containers..."
docker compose down 2>/dev/null || true

# Build and start services
echo "🔨 Building and starting services..."
docker compose build --no-cache
docker compose up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 30

# Check status
echo "🔍 Checking service status..."
docker compose ps

echo ""
echo "✅ Deployment complete!"
echo ""
echo "🌐 Airflow UI: http://localhost:8082"
echo "📊 PostgreSQL: localhost:5437"
echo ""
echo "📋 Next steps:"
echo "1. Go to Airflow UI and trigger 'setup_database_connections'"
echo "2. Trigger 'setup_sides_table' (one-time)"
echo "3. The 'populate_sources_table' DAG will run daily at 1 AM" 