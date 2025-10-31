FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY server.py .
COPY query_builder.py .

# Expose MCP port
EXPOSE 8000

# Run the FastMCP server
CMD ["python", "server.py"]
