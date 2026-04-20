FROM bitnami/spark:3.1.1

USER root





# Copy your custom jar into the Spark jars directory
COPY ./jars/postgresql-42.7.7.jar /opt/bitnami/spark/jars/
COPY ./jars/snowflake-jdbc-3.13.31.jar /opt/bitnami/spark/jars/
COPY ./jars/spark-snowflake_2.12-2.12.0-spark_3.3.jar /opt/bitnami/spark/jars/

USER 1001
