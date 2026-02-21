FROM python:3.12-slim AS builder

WORKDIR /install

COPY requirements.txt /requirements.txt
RUN pip install --prefix=/install -r /requirements.txt

FROM python:3.12-slim AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

COPY --from=builder /install /usr/local
COPY . ./apps/web

EXPOSE 8501

CMD ["streamlit", "run", "apps/web/main.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
