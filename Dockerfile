FROM python:3.10.4-slim-buster

ENV TZ_TIMEZONE=/etc/timezone \
    TZ_LOCALTIME=/etc/localtime \
    TZ_ZONEINFO=/usr/share/zoneinfo \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY ./app.py /app
COPY ./requirements_dash.txt /app

RUN ln -snf $TZ_ZONEINFO/$TZ $TZ_LOCALTIME \
	&& echo $TZ > $TZ_TIMEZONE \
    && pip install -r /app/requirements.txt \
    && pip install prefect>=2.0b

EXPOSE 8501

ENTRYPOINT ["streamlit", "run"]

CMD ["app.py"]
