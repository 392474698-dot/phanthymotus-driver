FROM bj-warehouse.tencentcloudcr.com/phanthy-motus/ros-base:latest

WORKDIR /work

RUN pip3 install --no-cache-dir \
    py_trees

ENV PYTHONUNBUFFERED=1

COPY main.py config.yaml /work/

EXPOSE 15800

CMD ["python3", "/work/main.py"]
