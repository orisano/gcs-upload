FROM scratch
ENTRYPOINT ["/gcs-upload"]
COPY gcs-upload /
