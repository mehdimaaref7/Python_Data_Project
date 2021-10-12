FROM debian:10
#install dependecies 
RUN apt-get -y update && apt-get -y dist-upgrade
RUN apt-get -y install python3
RUN apt-get -y install python3-pandas
RUN apt-get -y install python3-requests
#copy source code 
COPY main3.py /tmp
#run the coe 
CMD python3 /tmp/main3.py
