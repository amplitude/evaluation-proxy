FROM gradle:8-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle buildFatJar --no-daemon

FROM amazoncorretto:17-alpine
EXPOSE 3546:3546
EXPOSE 9090:9090
EXPOSE 9999:9999
RUN mkdir /amp
COPY --from=build /home/gradle/src/service/build/libs/service-all.jar /amp/evaluation-proxy.jar
ENV JAVA_OPTS="-Dcom.sun.management.jmxremote \
               -Dcom.sun.management.jmxremote.port=9999 \
               -Dcom.sun.management.jmxremote.authenticate=false \
               -Dcom.sun.management.jmxremote.ssl=false \
               -Dcom.sun.management.jmxremote.rmi.port=9999 \
               -Djava.rmi.server.hostname=0.0.0.0"

ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar /amp/evaluation-proxy.jar"]