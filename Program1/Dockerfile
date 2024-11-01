FROM python:3.9
WORKDIR /workspace
COPY . .
RUN pip install -r requirements.txt
ENV API_URL="https://u8whitimu7.execute-api.ap-southeast-1.amazonaws.com/prod"
ENTRYPOINT ["python", "main.py"]