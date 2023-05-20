FROM gcr.io/distroless/static
ENTRYPOINT ["/gcs-upload"]
COPY gcs-upload /
