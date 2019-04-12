FROM ${java.image}

COPY maven/ /maven

COPY entrypoint.sh /entrypoint.sh

RUN chmod u+x /entrypoint.sh

EXPOSE ${container.port}

ENV SERVICE_JAR ${service.jar}

ENTRYPOINT ["sh", "/entrypoint.sh"]

CMD ["server"]
