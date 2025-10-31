#!/bin/bash

# DOREMUS MCP Server - Startup Script

set -e

echo "╔════════════════════════════════════════════════════════════╗"
echo "║        DOREMUS Music Knowledge Graph MCP Server            ║"
echo "╚════════════════════════════════════════════════════════════╝"
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Error: Docker is not installed"
    echo "Please install Docker from https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ Error: docker-compose is not installed"
    echo "Please install docker-compose from https://docs.docker.com/compose/install/"
    exit 1
fi

echo "✅ Docker and docker-compose found"
echo ""

# Build and start the server
echo "🔨 Building Docker image..."
docker-compose build

echo ""
echo "🚀 Starting MCP server..."
docker-compose up -d

echo ""
echo "⏳ Waiting for server to be ready..."
sleep 5

# Check if server is running
if docker-compose ps | grep -q "Up"; then
    echo ""
    echo "✅ Server is running!"
    echo ""
    echo "📍 MCP Endpoint: http://127.0.0.1:8000/mcp"
    echo ""
    echo "📚 Documentation:"
    echo "   - README: ./README.md"
    echo "   - SPARQL Guide: ./ENDPOINT_GUIDE.md"
    echo "   - Example Queries: ./cq.json"
    echo ""
    echo "🧪 Test the server:"
    echo "   docker-compose exec doremus-mcp python test_server.py"
    echo ""
    echo "📋 View logs:"
    echo "   docker-compose logs -f"
    echo ""
    echo "🛑 Stop the server:"
    echo "   docker-compose down"
    echo ""
else
    echo ""
    echo "❌ Server failed to start. Check logs:"
    echo "   docker-compose logs"
    exit 1
fi
