# ---- Build stage ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY . .
# -DskipTests: test tích hợp cần Testcontainers (tức cần Docker), không chạy được bên trong bước
# build image. Chúng được chạy ở `mvn verify` trên máy dev và trong CI.
RUN mvn -q -pl services/bank-adapter -am clean package -DskipTests

# ---- Run stage ----
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/services/bank-adapter/target/*.jar app.jar
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
