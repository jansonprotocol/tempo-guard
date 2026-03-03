FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential libpq-dev libffi-dev \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir --no-deps soccerdata==1.3.0

COPY backend/app ./app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

ENV HTTP_PROXY=
ENV HTTPS_PROXY=
ENV ALL_PROXY=
ENV http_proxy=
ENV https_proxy=
ENV all_proxy=
ENV NO_PROXY=*

fastapi
uvicorn
pydantic
sqlalchemy
psycopg2-binary
python-dotenv
passlib[argon2]
argon2-cffi
pyjwt
requests
numpy==1.26.4
pandas==2.2.2
requests-cache
lxml
pyarrow
httpx==0.27.2
