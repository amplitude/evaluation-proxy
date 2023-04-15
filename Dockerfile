FROM gradle:8-jdk17 AS build
COPY --chown=gradle:gradle . /home/gradle/src
WORKDIR /home/gradle/src
RUN gradle buildFatJar --no-daemon

FROM openjdk:17-alpine
EXPOSE 3546:3546
RUN mkdir /amp
COPY --from=build /home/gradle/src/build/libs/*-all.jar /amp/evaluation-proxy.jar
ENTRYPOINT ["java","-jar","/app/evaluation-proxy.jar"]
