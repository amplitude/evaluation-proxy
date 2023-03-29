FROM gradle:7-jdk11 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle buildFatJar --no-daemon

FROM openjdk:11
EXPOSE 3546:3546
RUN mkdir /app
COPY --from=build /home/gradle/src/build/libs/*-all.jar /app/experiment-local-proxy.jar
ENTRYPOINT ["java","-jar","/app/experiment-local-proxy.jar"]
