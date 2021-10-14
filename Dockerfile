FROM ghcr.io/binkhq/python:3.9

WORKDIR /app
ADD . .

RUN pipenv install --deploy --system --ignore-pipfile

ENTRYPOINT [ "linkerd-await", "--" ]
CMD [ "gunicorn", "--error-logfile=-", "--access-logfile=-", \
      "--bind=0.0.0.0:6502", "--timeout=600", "main:app" ]
