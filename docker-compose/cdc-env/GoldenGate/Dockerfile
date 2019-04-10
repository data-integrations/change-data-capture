FROM oracle/goldengate-standard:18.1.0.0.0

COPY ./dirprm /u01/app/ogg/dirprm
RUN chmod -R 777 /u01/app/ogg/dirprm \
  && mkdir /u01/app/ogg/dirdat && chown -R oracle:oinstall /u01/app/ogg/dirdat
