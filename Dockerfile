FROM google/cloud-sdk:slim

COPY --from=ghcr.io/astral-sh/uv:0.11.6 /uv /usr/local/bin/uv

WORKDIR /app
COPY download.py .
RUN mkdir -p /app/data

ENTRYPOINT ["uv", "run", "download.py"]
