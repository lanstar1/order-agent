FROM python:3.11-alpine

RUN apk add --no-cache git curl docker-cli

WORKDIR /app
COPY server.py /app/server.py
RUN mkdir -p /app/logs /app/repo

EXPOSE 9000

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:9000/health || exit 1

CMD ["python3", "server.py"]
