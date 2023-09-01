# get image from spark-operator repository 
# https://googlecloudplatform.github.io/spark-on-k8s-operator
ARG SPARK_VERSION=v3.3.2
FROM apache/spark-py:${SPARK_VERSION}
LABEL org.opencontainers.image.authors="onofre.felxi@domrock.ai"
LABEL EMAIL = onofre.felix@domrock.ai

# using root user
USER root:root

# create the directory that will store the spark jobs
RUN mkdir -p /app

# copy spark jobs local to image
COPY . /app/


# copy jars files
COPY  ./jars/* /opt/spark/jars/

# pip install
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir delta-spark==2.3.0 \
                               boto3==1.26.114 \
                               pyyaml==6.0.1

# set python3
ENV PYSPARK_PYTHON=/usr/bin/python3

# set main work directory
WORKDIR /app/

CMD ["python3", "main.py"]
