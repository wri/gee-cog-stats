FROM google/cloud-sdk:slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app
COPY download.py .
RUN mkdir -p /app/data

ENTRYPOINT ["uv", "run", "download.py"]
