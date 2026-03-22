FROM apache/spark:3.5.1-python3
USER root
RUN pip install --no-cache-dir jupyterlab  notebook  delta-spark==3.2.0  boto3 --quiet

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/


RUN mkdir -p /opt/notebooks
WORKDIR /opt/notebooks
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=", "--NotebookApp.password=", "--allow-root"]