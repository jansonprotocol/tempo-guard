FROM python:3.11-slim

WORKDIR /app

# System deps: build-essential for compiling wheels, libpq-dev for Postgres,
# libffi-dev for argon2-cffi (via cffi)
RUN apt-get update && apt-get install -y \
    build-essential libpq-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

ENV HTTP_PROXY=
ENV HTTPS_PROXY=
ENV ALL_PROXY=
ENV http_proxy=
ENV https_proxy=
ENV all_proxy=
ENV NO_PROXY=*
