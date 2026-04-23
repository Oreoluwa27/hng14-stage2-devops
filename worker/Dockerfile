FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY worker.py ./

RUN groupadd -g 1001 appgroup && \
    useradd -m -u 1001 -g appgroup appuser

USER appuser

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD pgrep -f "python worker.py" > /dev/null || exit 1
  
CMD ["python", "worker.py"]