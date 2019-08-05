#!/bin/sh

KEY_PASSWORD=$1
KEYSTORE_PASSWORD=$2
TRUSTSTORE_PASSWORD=$3

OUTPUT_GEN=secrets

mkdir -p $OUTPUT_GEN

echo '[extended]\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyAgreement' > $OUTPUT_GEN/openssl.cnf

## Create certificate authority (CA)
openssl req -new -x509 -keyout $OUTPUT_GEN/ca_key.pem -out $OUTPUT_GEN/ca_cert.pem -days 365 -passin pass:$KEY_PASSWORD -passout pass:$KEY_PASSWORD -subj "/CN=localhost"

## Create client keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-client.jks -genkey -alias selfsigned -dname "CN=localhost" -storetype PKCS12 -keyalg RSA -keysize 2048 -validity 365 -storepass $KEYSTORE_PASSWORD -keypass $KEY_PASSWORD
openssl pkcs12 -in $OUTPUT_GEN/keystore-client.jks -nocerts -nodes -passin pass:$KEYSTORE_PASSWORD -out $OUTPUT_GEN/client_key.pem

## Create server keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-server.jks -genkey -alias selfsigned -dname "CN=localhost" -storetype PKCS12 -keyalg RSA -keysize 2048 -validity 365 -storepass $KEYSTORE_PASSWORD -keypass $KEY_PASSWORD
openssl pkcs12 -in $OUTPUT_GEN/keystore-server.jks -nocerts -nodes -passin pass:$KEYSTORE_PASSWORD -out $OUTPUT_GEN/server_key.pem

## Sign client certificate
keytool -noprompt -keystore $OUTPUT_GEN/keystore-client.jks -alias selfsigned -certreq -file $OUTPUT_GEN/client_csr.pem -storepass $KEYSTORE_PASSWORD
openssl x509 -extfile $OUTPUT_GEN/openssl.cnf -extensions extended -req -CA $OUTPUT_GEN/ca_cert.pem -CAkey $OUTPUT_GEN/ca_key.pem -in $OUTPUT_GEN/client_csr.pem -out $OUTPUT_GEN/client_cert.pem -days 365 -CAcreateserial -passin pass:$KEY_PASSWORD

## Sign server certificate
keytool -noprompt -keystore $OUTPUT_GEN/keystore-server.jks -alias selfsigned -certreq -file $OUTPUT_GEN/server_csr.pem -storepass $KEYSTORE_PASSWORD
openssl x509 -extfile $OUTPUT_GEN/openssl.cnf -extensions extended -req -CA $OUTPUT_GEN/ca_cert.pem -CAkey $OUTPUT_GEN/ca_key.pem -in $OUTPUT_GEN/server_csr.pem -out $OUTPUT_GEN/server_cert.pem -days 365 -CAcreateserial -passin pass:$KEY_PASSWORD

## Import CA and client signed certificate into client keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-client.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $KEYSTORE_PASSWORD
keytool -noprompt -keystore $OUTPUT_GEN/keystore-client.jks -alias selfsigned -import -file $OUTPUT_GEN/client_cert.pem -storepass $KEYSTORE_PASSWORD

## Import CA and server signed certificate into server keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-server.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $KEYSTORE_PASSWORD
keytool -noprompt -keystore $OUTPUT_GEN/keystore-server.jks -alias selfsigned -import -file $OUTPUT_GEN/server_cert.pem -storepass $KEYSTORE_PASSWORD

## Import CA into client truststore
keytool -noprompt -keystore $OUTPUT_GEN/truststore-client.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $TRUSTSTORE_PASSWORD

## Import CA into server truststore
keytool -noprompt -keystore $OUTPUT_GEN/truststore-server.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $TRUSTSTORE_PASSWORD


