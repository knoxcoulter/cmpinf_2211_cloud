FROM apache/spark:3.5.3
USER root
# Set environment variables
ENV HOME=/root
ENV SPARK_USER=root
# Create .ivy2 directory and set permissions
RUN mkdir -p /root/.ivy2/local && \
chmod -R 777 /root/.ivy2
# Install Python and packages
RUN apt-get update && apt-get install -y python3-pip && \
pip3 install numpy pandas scikit-learn
USER spark