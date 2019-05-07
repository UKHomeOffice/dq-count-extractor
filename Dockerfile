FROM python:3.7.2
RUN useradd python -u 1001
WORKDIR /app
RUN pip install boto3
RUN pip install pathos
COPY dqCountExtractor.py .
USER 1001
CMD python dqCountExtractor.py
