FROM ${java.image}

COPY maven/ /maven

COPY entrypoint_server.sh /entrypoint_server.sh
COPY entrypoint_sidecar.sh /entrypoint_sidecar.sh

RUN chmod u+x /entrypoint_server.sh
RUN chmod u+x /entrypoint_sidecar.sh

EXPOSE ${container.port}

ENV SERVICE_JAR ${service.jar}

ENTRYPOINT ["sh"]

CMD ["/entrypoint_server.sh"]
