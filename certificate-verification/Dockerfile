FROM egovio/alpine-node-builder-14:yarn AS build
RUN apk update && apk upgrade
RUN apk add --no-cache git>2.30.0
ARG WORK_DIR
WORKDIR /app
ENV NODE_OPTIONS "--max-old-space-size=1792"

COPY ${WORK_DIR} .

RUN cd certificate-verification && yarn install
RUN cd certificate-verification && yarn run-script build

FROM nginx:mainline-alpine
ENV WORK_DIR=/var/certificate-verification

RUN mkdir -p ${WORK_DIR}

COPY --from=build /app/certificate-verification/build ${WORK_DIR}/
COPY --from=build /app/certificate-verification/nginx.conf /etc/nginx/conf.d/default.conf
