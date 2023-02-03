# python 3.8 gives problems
FROM python:3.9
WORKDIR /code
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt
COPY app /code/app
#CMD ["python", "app.main:app", "--host", "0.0.0.0", "--port", "80"]
