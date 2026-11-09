FROM gradle:8-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle buildFatJar --no-daemon

FROM amazoncorretto:17-alpine
EXPOSE 3546
EXPOSE 9090
RUN mkdir /amp \
    && addgroup -S -g 10001 amp \
    && adduser -S -u 10001 -G amp -H -s /sbin/nologin amp
COPY --from=build --chown=amp:amp /home/gradle/src/service/build/libs/service-all.jar /amp/evaluation-proxy.jar

# JAVA_OPTS is intentionally empty by default. Remote JMX used to be enabled here
# without authentication or TLS; it is now opt-in. To enable JMX for debugging,
# set JAVA_OPTS at deploy time, e.g.:
#   -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 \
#   -Dcom.sun.management.jmxremote.rmi.port=9999 -Djava.rmi.server.hostname=127.0.0.1 \
#   -Dcom.sun.management.jmxremote.authenticate=true -Dcom.sun.management.jmxremote.ssl=true
# and restrict network access to the JMX port.
ENV JAVA_OPTS=""

USER amp:amp
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /amp/evaluation-proxy.jar"]
