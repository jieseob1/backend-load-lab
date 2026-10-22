FROM eclipse-temurin:21-jre
WORKDIR /app
# 먼저 ./gradlew bootJar 로 build/libs/app.jar 를 만듭니다.
COPY build/libs/app.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
