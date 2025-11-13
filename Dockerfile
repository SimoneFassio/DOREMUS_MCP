FROM python:3.11-slim

WORKDIR /app

# Install Poetry
RUN pip install poetry==1.8.3
#--no-cache-dir

# Copy dependency files
COPY pyproject.toml poetry.lock* ./

# Configure Poetry to not create virtual environment (we're already in a container)
RUN poetry config virtualenvs.create false

# Install only server dependencies (no eval)
RUN poetry install --only main,server --no-interaction --no-ansi --no-root

# Copy application files
COPY src/server ./src/server
COPY data/ ./data/

# Copy tests if needed for validation
COPY tests ./tests

# Install the project itself
RUN poetry install --only-root --no-interaction --no-ansi

# Expose MCP port
EXPOSE 8000

# Run the FastMCP server
CMD ["python", "-m", "src.server.server"]

