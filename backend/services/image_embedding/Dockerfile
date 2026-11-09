FROM python:3.14-bookworm
EXPOSE 11435
WORKDIR /app

COPY pyproject.toml ./
COPY libs ./libs
COPY services/image_embedding ./services/image_embedding

RUN pip install --no-cache-dir -U pip \
    && pip install --no-cache-dir \
        torch==2.12.1 \
        torchvision==0.27.1 \
        --index-url https://download.pytorch.org/whl/cpu \
    && pip install --no-cache-dir -e ".[image-embedding-server]"

CMD ["python", "-m", "services.image_embedding.main"]
