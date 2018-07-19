#!/bin/bash

echo "ssl_enable=YES" >> /etc/vsftpd/vsftpd.conf
echo "rsa_cert_file=/etc/pki/tls/certs/vsftpd.cert.pem" >> /etc/vsftpd/vsftpd.conf
echo "rsa_private_key_file=/etc/pki/tls/private/vsftpd.key.pem" >> /etc/vsftpd/vsftpd.conf
echo "ssl_sslv2=NO" >> /etc/vsftpd/vsftpd.conf
echo "ssl_sslv3=NO" >> /etc/vsftpd/vsftpd.conf
echo "ssl_tlsv1=YES" >> /etc/vsftpd/vsftpd.conf
echo "force_local_data_ssl=YES" >> /etc/vsftpd/vsftpd.conf
echo "force_local_logins_ssl=YES" >> /etc/vsftpd/vsftpd.conf

/usr/sbin/run-vsftpd.sh
