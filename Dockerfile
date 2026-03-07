FROM python:3.11-slim

# Set timezone dynamically if requested, otherwise default to UTC
ENV TZ=UTC

# Update OS and install Cron + curl (required for heartbeat pings)
RUN apt-get update \
    && apt-get install -y --no-install-recommends cron curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory inside container
WORKDIR /app

# Copy application dependencies and install
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source repository
COPY . /app/

# Ensure logs directory exists and pre-create the log file so tail -f won't fail
RUN mkdir -p /app/logs && touch /app/logs/analyze.log

# Normalize line endings and set execute bit on all shell scripts (Windows host safety)
RUN find /app/scripts -type f -name '*.sh' -exec sed -i 's/\r$//' {} + \
    && chmod +x /app/scripts/*.sh

# Normalize cron file line endings (in case host is Windows)
RUN if [ -f /app/crontab ]; then sed -i 's/\r$//' /app/crontab; fi

# Setup Cronjob File into the container's cron.d
COPY crontab /etc/cron.d/repeat-customer-cron

# Give execution rights and apply cron job
RUN chmod 0644 /etc/cron.d/repeat-customer-cron \
    && crontab /etc/cron.d/repeat-customer-cron

# Dump container env vars into /etc/environment so system cron can read them,
# then start cron and stream the job log (job output goes to /app/logs/analyze.log).
# Note: mkdir + touch run at container start so they happen AFTER the volume mount,
#       which would otherwise shadow any files created during the image build.
CMD ["sh", "-c", "printenv >> /etc/environment && mkdir -p /app/logs && touch /app/logs/analyze.log && cron && echo 'Cron daemon started.' && tail -f /app/logs/analyze.log"]
