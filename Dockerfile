FROM jeanblanchard/java:8

ENV DATA_DIR /data
VOLUME /data
ADD trail-register.jar /trail-register.jar
EXPOSE 4567

CMD java -jar trail-register.jar