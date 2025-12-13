FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app


ENV UV_COMPILE_BYTECODE=1


COPY pyproject.toml uv.lock ./


RUN uv sync --frozen --no-install-project --no-dev


COPY . .


RUN uv sync --frozen --no-dev


CMD ["uv", "run", "fastapi", "run", "src/voc/main.py", "--port", "80"]
