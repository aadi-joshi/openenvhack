# DB-ER OpenEnv Environment - Docker Image
#
# Build:
#   docker build -t db-er .
#
# Run (server mode):
#   docker run -p 7860:7860 db-er
#
# Run (inference mode):
#   docker run --rm \
#     -e API_BASE_URL=https://api-inference.huggingface.co/v1 \
#     -e MODEL_NAME=meta-llama/Llama-3.3-70B-Instruct \
#     -e HF_TOKEN=hf_your_token \
#     db-er python inference.py
#
# Hugging Face Spaces: set PORT env var (Spaces uses 7860 by default).
# 

FROM python:3.11-slim

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user required by HF Spaces
RUN useradd -m -u 1000 user
ENV PATH="/home/user/.local/bin:$PATH"

WORKDIR /home/user/app

# Install dependencies as root into system paths (avoids --user permission issues)
COPY requirements.txt pyproject.toml ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project files and set ownership
COPY . .
RUN chown -R user:user /home/user/app

# Switch to non-root user for runtime
USER user

# Expose the FastAPI server port (Hugging Face Spaces default is 7860)
ENV PORT=7860
EXPOSE 7860

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Start the DB-ER environment server
CMD ["sh", "-c", "uvicorn server.app:app --host 0.0.0.0 --port ${PORT} --workers 1"]
