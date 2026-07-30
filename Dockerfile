FROM python:3.10-slim

WORKDIR /work

RUN pip3 install --no-cache-dir \
    pyyaml py_trees

ENV PYTHONUNBUFFERED=1

COPY main.py config.yaml /work/

EXPOSE 15800

CMD ["python3", "/work/main.py"]
