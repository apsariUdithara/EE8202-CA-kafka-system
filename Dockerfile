# syntax=docker/dockerfile:1
#
# Multi-stage build: wheels are compiled in the builder, and only the runtime
# artefacts are copied into the final image, which runs as a non-root user.

FROM python:3.12-slim AS builder

WORKDIR /build
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1

COPY pyproject.toml README.md ./
COPY src/ ./src/
COPY schemas/ ./schemas/

RUN python -m pip install --upgrade pip build \
    && python -m build --wheel --outdir /dist


FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

RUN useradd --create-home --uid 10001 appuser

COPY --from=builder /dist/*.whl /tmp/
RUN python -m pip install /tmp/*.whl && rm -f /tmp/*.whl

USER appuser
WORKDIR /home/appuser

# Overridden by docker-compose; the consumer is the more useful default.
CMD ["order-consumer"]
