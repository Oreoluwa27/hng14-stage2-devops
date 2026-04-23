FROM python:3.12-slim AS builder
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py ./

RUN groupadd -g 1001 appgroup && \
    useradd -m -u 1001 -g appgroup appuser

USER appuser
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8000/jobs || exit 1
  
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]