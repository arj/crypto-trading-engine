FROM node

WORKDIR /connector/
COPY src/* /connector/
RUN npm install
ENTRYPOINT ["/connector/start.sh"]
