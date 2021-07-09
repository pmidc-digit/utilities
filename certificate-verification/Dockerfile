FROM node:12-alpine3.9 AS build  
ARG WORK_DIR
WORKDIR /app
ENV NODE_OPTIONS "--max-old-space-size=1792"
ENV NPM_CONFIG_PREFIX=/tmp/.npm-global
ENV PATH=$PATH:/tmp/.npm-global/bin
ENV HOME=/tmp
# copy the project files
COPY ${WORK_DIR} .
RUN apk --update add git
RUN npm i && npm run-script build
# Create runtime image
FROM nginx:mainline-alpine 
ENV WEB_DIR=/var/certificate-verification
RUN mkdir -p ${WEB_DIR}
COPY --from=build /app/certificate-verification/build ${WEB_DIR}/
COPY --from=build /app/certificate-verification/nginx.conf /etc/nginx/conf.d/default.conf
