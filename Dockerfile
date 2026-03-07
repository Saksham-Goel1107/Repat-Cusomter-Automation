# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: dependency builder
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System libs needed to compile pandas/numpy wheels.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --no-cache-dir --prefix=/install -r requirements.txt


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2: runtime image
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# ── Install supercronic (Docker-native cron — logs to stdout, no root cron) ──
# pinned SHA for supply-chain safety
ENV SUPERCRONIC_VERSION=0.2.29

# Install curl and download supercronic. The pinned SHA check caused failures in
# some build environments; use a simple download + chmod to avoid broken builds.
RUN apt-get update && apt-get install -y --no-install-recommends curl \
 && curl -fsSLo /usr/local/bin/supercronic \
     "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
 && chmod +x /usr/local/bin/supercronic \
 && rm -rf /var/lib/apt/lists/*

# ── Copy installed Python packages from builder stage ────────────────────────
COPY --from=builder /install /usr/local

# ── Create non-root user ──────────────────────────────────────────────────────
RUN groupadd -r worker && useradd -r -g worker -d /app -s /sbin/nologin worker

# ── Application code ──────────────────────────────────────────────────────────
WORKDIR /app
COPY src/ ./src/
COPY scripts/run_and_heartbeat.sh /app/cron/run_and_heartbeat.sh

# If a service account key is present at build time, copy it into the image so
# runtime can find it without extra env wiring. Note: embedding credentials in
# images is only appropriate for controlled environments; otherwise mount a
# secret at runtime instead.
COPY service.json /app/credentials/service.json
RUN chmod 600 /app/credentials/service.json || true

# ── Directories for credentials, logs, charts, cron ──────────────────────────
RUN mkdir -p /app/credentials /app/logs /app/cron \
 && chown -R worker:worker /app

# ── Crontab for supercronic ────────────────────────────────────────────────────
# Default: run daily at 01:00 UTC (06:30 IST).
# Override by setting CRON_SCHEDULE and rebuilding, or mount a custom crontab.
ARG CRON_SCHEDULE="0 1 * * *"
RUN echo "${CRON_SCHEDULE} cd /app && /app/cron/run_and_heartbeat.sh >> /app/logs/run.log 2>&1" \
    > /app/cron/crontab \
 && chown worker:worker /app/cron/crontab
RUN chmod +x /app/cron/run_and_heartbeat.sh || true

# ── Health-check: verify Python + imports are OK ──────────────────────────────
HEALTHCHECK --interval=60s --timeout=10s --start-period=10s --retries=3 \
    CMD python -c "from src.analysis import run_full_analysis; print('OK')" || exit 1

USER worker

# ── Default command: run supercronic with our crontab ─────────────────────────
CMD ["/usr/local/bin/supercronic", "/app/cron/crontab"]
