#SET UP FROM PRODUCTION BASE IMAGE
FROM ghcr.io/hcdp/task-base:latest
LABEL org.opencontainers.image.source="https://github.com/hcdp/preliminary_madis_hourly"
LABEL org.opencontainers.image.description="Image for the Preliminary Madis Hourly Workflow."

RUN pip install --upgrade pip

COPY code/ /workspace/code/
COPY envs/prod.env /workspace/envs/prod.env
COPY containers/task.sh /workspace/task.sh