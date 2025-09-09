FROM public.ecr.aws/glue/aws-glue-libs:5

# Install Python packages
RUN pip install --no-cache-dir openpyxl

# Install PostgreSQL JDBC driver
RUN mkdir -p /home/hadoop/jars && \
    curl -L -o /home/hadoop/jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# RUN THIS COMAND: sudo docker build -t glue-libs-with-openpyxl-postgres .

# # Switch to root to create jars directory
# USER root
# RUN mkdir -p /home/hadoop/jars

# # Define versions & base URL so they're easy to update
# ENV SPARK_EXCEL_VERSION=0.13.7 \
#     POI_VERSION=5.2.2 \
#     COMMONS_COLLECTIONS_VERSION=4.4 \
#     JARS_DIR=/home/hadoop/jars \
#     MAVEN_BASE=https://repo1.maven.org/maven2

# # Download JAR dependencies
# RUN curl -L -o $JARS_DIR/spark-excel_2.12-${SPARK_EXCEL_VERSION}.jar \
#         $MAVEN_BASE/com/crealytics/spark-excel_2.12/${SPARK_EXCEL_VERSION}/spark-excel_2.12-${SPARK_EXCEL_VERSION}.jar && \
#     curl -L -o $JARS_DIR/poi-ooxml-${POI_VERSION}.jar \
#         $MAVEN_BASE/org/apache/poi/poi-ooxml/${POI_VERSION}/poi-ooxml-${POI_VERSION}.jar && \
#     curl -L -o $JARS_DIR/poi-${POI_VERSION}.jar \
#         $MAVEN_BASE/org/apache/poi/poi/${POI_VERSION}/poi-${POI_VERSION}.jar && \
#     curl -L -o $JARS_DIR/commons-collections4-${COMMONS_COLLECTIONS_VERSION}.jar \
#         $MAVEN_BASE/org/apache/commons/commons-collections4/${COMMONS_COLLECTIONS_VERSION}/commons-collections4-${COMMONS_COLLECTIONS_VERSION}.jar

# Switch back to hadoop user (needed by Glue)
# USER hadoop
