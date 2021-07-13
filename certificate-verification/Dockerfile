FROM node:12-alpine3.9 AS build  
ARG WORK_DIR
WORKDIR /app
ENV NODE_OPTIONS "--max-old-space-size=1792"

# copy the project files
COPY ${WORK_DIR} .
RUN apk --update add git
RUN npm i && npm run-script build

# Create runtime image
FROM nginx:mainline-alpine 
ENV WEB_DIR=/var/web/certificate-verification
RUN mkdir -p ${WEB_DIR}
COPY --from=build /app/build ${WEB_DIR}/
COPY --from=build /app/nginx.conf /etc/nginx/conf.d/default.conf
