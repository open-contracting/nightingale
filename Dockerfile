FROM python:3.12

RUN groupadd -r runner && useradd --no-log-init -r -g runner runner

COPY . /tmp/build
RUN pip install --no-cache-dir /tmp/build && rm -rf /tmp/build

WORKDIR /workdir
USER runner:runner

ENTRYPOINT ["ocdsnightingale"]
