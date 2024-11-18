FROM wentaojin/dbms1804:go1.22.9

# Create a working directory
ENV APP_WORK_DIR=/dbms
RUN mkdir -p ${APP_WORK_DIR}
WORKDIR ${APP_WORK_DIR}

# Copy code
COPY . .
RUN go mod download

RUN chmod +x ${APP_WORK_DIR}/build.sh

CMD ["/dbms/build.sh"]
