FROM python:3.11-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry==2.2.1
#--no-cache-dir

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Configure Poetry to not create virtual environment (we're already in a container)
RUN poetry config virtualenvs.create false

# Install dependencies (no root) to cache them
RUN poetry install --only main --no-interaction --no-ansi --no-root

# Copy application files
COPY src/server ./src/server

# Install the project itself (now that source is present)
RUN poetry install --only main --no-interaction --no-ansi

# Copy tests if needed for validation
COPY tests ./tests

# Expose MCP port
EXPOSE 8000

# Run the FastMCP server
CMD ["python", "-m", "server.main"]

