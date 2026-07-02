FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends g++ make python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN make kvserver

EXPOSE 8080 9006 19021 19022 19023 19024 19025

CMD ["python3", "tools/dashboard_server.py"]
