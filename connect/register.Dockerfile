FROM alpine:3.20

RUN apk add --no-cache bash curl gettext

COPY register-connector.sh /register-connector.sh
COPY snowflake-sink-connector.json /connect/snowflake-sink-connector.json

ENTRYPOINT ["bash", "/register-connector.sh"]
