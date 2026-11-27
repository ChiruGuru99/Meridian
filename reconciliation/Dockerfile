FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
RUN useradd --system --uid 10001 app && mkdir /data && chown app /data
WORKDIR /app
COPY pyproject.toml ./
COPY recon recon
RUN pip install --no-cache-dir .

USER app
EXPOSE 8083
ENTRYPOINT ["python", "-m", "recon"]
CMD ["serve"]
