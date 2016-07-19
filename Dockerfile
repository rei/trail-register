FROM java:8-jre-alpine

ENV DATA_DIR /data
VOLUME /data
ADD trail-register.jar /trail-register.jar
EXPOSE 4567

CMD java -Xmx512m -jar trail-register.jar
