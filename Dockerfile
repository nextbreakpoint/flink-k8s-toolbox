FROM gradle:6.6.1-jdk11 AS build
ADD . /src
WORKDIR /src
RUN gradle --no-daemon clean build test copyRuntimeDeps

FROM oracle/graalvm-ce:20.3.0-java11 as native-image
COPY --from=build /src/build/libs/* /build/libs/
RUN gu install native-image
WORKDIR /
RUN native-image --verbose -J-Xmx4G -cp $(find build/libs -name "*.jar" -print | sed "s/.jar/.jar:/g" | tr -d '\n' | sed "s/:$//g") -H:+StaticExecutableWithDynamicLibC -H:Name=flinkctl

FROM gcr.io/distroless/base
COPY --from=native-image flinkctl /flinkctl
EXPOSE 4444 8080
ENTRYPOINT ["/flinkctl"]
