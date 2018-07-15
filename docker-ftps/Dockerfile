FROM fauria/vsftpd

COPY ca.key.pem /etc/pki/tls/private/vsftpd.key.pem
COPY ca.cert.pem /etc/pki/tls/certs/vsftpd.cert.pem

COPY run-vsftpd-with-ssl.sh /usr/sbin/
RUN chmod +x /usr/sbin/run-vsftpd-with-ssl.sh

CMD ["/usr/sbin/run-vsftpd-with-ssl.sh"]
