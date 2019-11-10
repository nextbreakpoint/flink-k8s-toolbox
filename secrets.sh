#!/bin/sh

OPERATOR_HOST=$1
KEY_PASSWORD=$2
KEYSTORE_PASSWORD=$3
TRUSTSTORE_PASSWORD=$4

USAGE="Usage: secrets.sh operator-host key-password keystore-password truststore-password"

if [[ -z "$OPERATOR_HOST" ]]; then
    echo ${USAGE}
    exit 1
fi

if [[ -z "$KEY_PASSWORD" ]]; then
    echo ${USAGE}
    exit 1
fi

if [[ -z "$KEYSTORE_PASSWORD" ]]; then
    echo ${USAGE}
    exit 1
fi

if [[ -z "$TRUSTSTORE_PASSWORD" ]]; then
    echo ${USAGE}
    exit 1
fi

OUTPUT_GEN=secrets

if [[ -d "secrets" ]]; then
    echo "Warning: The directory secrets already exists. Please delete the directory and retry."
    exit 2
fi

mkdir -p $OUTPUT_GEN

cat <<EOF >$OUTPUT_GEN/openssl.cnf
[ req_ext ]
subjectAltName       = @alt_names
extendedKeyUsage     = serverAuth,clientAuth
keyUsage             = digitalSignature,keyAgreement
[alt_names]
DNS.1                = localhost
DNS.2                = 127.0.0.1
DNS.3                = host.docker.internal
DNS.4                = ${OPERATOR_HOST}
EOF

## Create certificate authority (CA)
openssl req -new -x509 -keyout $OUTPUT_GEN/ca_key.pem -out $OUTPUT_GEN/ca_cert.pem -days 365 -passin pass:$KEY_PASSWORD -passout pass:$KEY_PASSWORD -subj "/CN=${OPERATOR_HOST}"

## Create operator cli keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -genkey -alias selfsigned -dname "CN=${OPERATOR_HOST}" -storetype PKCS12 -keyalg RSA -keysize 2048 -validity 365 -storepass $KEYSTORE_PASSWORD
openssl pkcs12 -in $OUTPUT_GEN/keystore-operator-cli.jks -nocerts -nodes -passin pass:$KEYSTORE_PASSWORD -out $OUTPUT_GEN/operator-cli_key.pem

## Create operator api keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-api.jks -genkey -alias selfsigned -dname "CN=${OPERATOR_HOST}" -storetype PKCS12 -keyalg RSA -keysize 2048 -validity 365 -storepass $KEYSTORE_PASSWORD
openssl pkcs12 -in $OUTPUT_GEN/keystore-operator-api.jks -nocerts -nodes -passin pass:$KEYSTORE_PASSWORD -out $OUTPUT_GEN/operator-api_key.pem

## Sign operator cli certificate
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -alias selfsigned -certreq -file $OUTPUT_GEN/operator-cli_csr.pem -storepass $KEYSTORE_PASSWORD
openssl x509 -extfile $OUTPUT_GEN/openssl.cnf -extensions req_ext -req -CA $OUTPUT_GEN/ca_cert.pem -CAkey $OUTPUT_GEN/ca_key.pem -in $OUTPUT_GEN/operator-cli_csr.pem -out $OUTPUT_GEN/operator-cli_cert.pem -days 365 -CAcreateserial -passin pass:$KEY_PASSWORD

## Sign operator api certificate
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-api.jks -alias selfsigned -certreq -file $OUTPUT_GEN/operator-api_csr.pem -storepass $KEYSTORE_PASSWORD
openssl x509 -extfile $OUTPUT_GEN/openssl.cnf -extensions req_ext -req -CA $OUTPUT_GEN/ca_cert.pem -CAkey $OUTPUT_GEN/ca_key.pem -in $OUTPUT_GEN/operator-api_csr.pem -out $OUTPUT_GEN/operator-api_cert.pem -days 365 -CAcreateserial -passin pass:$KEY_PASSWORD

## Import CA and operator cli signed certificate into operator cli keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $KEYSTORE_PASSWORD
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -alias selfsigned -import -file $OUTPUT_GEN/operator-cli_cert.pem -storepass $KEYSTORE_PASSWORD

## Import CA and operator api signed certificate into operator api keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-api.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $KEYSTORE_PASSWORD
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-api.jks -alias selfsigned -import -file $OUTPUT_GEN/operator-api_cert.pem -storepass $KEYSTORE_PASSWORD

## Import CA into operator cli truststore
keytool -noprompt -keystore $OUTPUT_GEN/truststore-operator-cli.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $TRUSTSTORE_PASSWORD

## Import CA into operator api truststore
keytool -noprompt -keystore $OUTPUT_GEN/truststore-operator-api.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $TRUSTSTORE_PASSWORD
