FROM gcr.io/distroless/static-debian11:latest
COPY cbt* /cbt
ENTRYPOINT ["/cbt"]
