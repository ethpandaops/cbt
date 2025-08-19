FROM gcr.io/distroless/static-debian12:latest
COPY cbt* /cbt
ENTRYPOINT ["/cbt"]
