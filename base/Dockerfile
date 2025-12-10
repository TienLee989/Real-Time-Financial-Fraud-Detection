FROM apache/airflow:2.9.0

USER root

# Cài Java (cho Spark)
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxrender1 \
    libxext6 \
    ffmpeg \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy file requirements trong chính thư mục base
COPY base/requirements.txt /tmp/base_requirements.txt

USER airflow
RUN pip install --no-cache-dir -r /tmp/base_requirements.txt && \
    # Fix lỗi kafka.vendor.six.moves không tồn tại trong kafka-python 2.0.1
    sed -i "s|from kafka.vendor.six.moves import range|from six.moves import range|" \
    /home/airflow/.local/lib/python3.*/site-packages/kafka/codec.py || true

# ================================
# 🔧 Copy scripts và cấp quyền thực thi (chuẩn reproducible)
# ================================
USER root
# Project FINANCIAL FRAUD
COPY projects/financial_fraud/scripts /opt/airflow/projects/financial_fraud/scripts
RUN chmod +x /opt/airflow/projects/financial_fraud/scripts/*.sh
COPY projects/financial_fraud/data /opt/airflow/projects/financial_fraud/data
COPY projects/financial_fraud/streamlit /opt/airflow/projects/financial_fraud/streamlit

# # Project ABSA STREAMING
# COPY projects/absa_streaming/scripts /opt/airflow/projects/absa_streaming/scripts
# RUN chmod +x /opt/airflow/projects/absa_streaming/scripts/*.sh
# COPY projects/absa_streaming/streamlit /opt/airflow/projects/absa_streaming/streamlit

# # Project VEHICLE COUNTING mới
# COPY projects/vehicle_count/scripts /opt/airflow/projects/vehicle_count/scripts
# COPY projects/vehicle_count/data /opt/airflow/projects/vehicle_count/data
# COPY projects/vehicle_count/streamlit /opt/airflow/projects/vehicle_count/streamlit
# RUN chmod +x /opt/airflow/projects/vehicle_count/scripts/*.sh

USER airflow
# Cài các thư viện cần thiết cho Streamlit
USER airflow
RUN pip install --no-cache-dir streamlit psycopg2-binary pandas