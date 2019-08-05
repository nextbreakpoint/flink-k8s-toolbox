#!/bin/sh

NAME=$1
KEY_PASSWORD=$2
KEYSTORE_PASSWORD=$3
TRUSTSTORE_PASSWORD=$4

USAGE="Usage: secrets.sh hostname key-password keystore-password truststore-password"

if [[ -z "$NAME" ]]; then
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

echo '[extended]\nextendedKeyUsage=serverAuth,clientAuth\nkeyUsage=digitalSignature,keyAgreement' > $OUTPUT_GEN/openssl.cnf

## Create certificate authority (CA)
openssl req -new -x509 -keyout $OUTPUT_GEN/ca_key.pem -out $OUTPUT_GEN/ca_cert.pem -days 365 -passin pass:$KEY_PASSWORD -passout pass:$KEY_PASSWORD -subj "/CN=${NAME}"

## Create operator cli keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -genkey -alias selfsigned -dname "CN=${NAME}" -storetype PKCS12 -keyalg RSA -keysize 2048 -validity 365 -storepass $KEYSTORE_PASSWORD -keypass $KEY_PASSWORD
openssl pkcs12 -in $OUTPUT_GEN/keystore-operator-cli.jks -nocerts -nodes -passin pass:$KEYSTORE_PASSWORD -out $OUTPUT_GEN/operator-cli_key.pem

## Create operator keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator.jks -genkey -alias selfsigned -dname "CN=${NAME}" -storetype PKCS12 -keyalg RSA -keysize 2048 -validity 365 -storepass $KEYSTORE_PASSWORD -keypass $KEY_PASSWORD
openssl pkcs12 -in $OUTPUT_GEN/keystore-operator.jks -nocerts -nodes -passin pass:$KEYSTORE_PASSWORD -out $OUTPUT_GEN/operator_key.pem

## Sign operator cli certificate
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -alias selfsigned -certreq -file $OUTPUT_GEN/operator-cli_csr.pem -storepass $KEYSTORE_PASSWORD
openssl x509 -extfile $OUTPUT_GEN/openssl.cnf -extensions extended -req -CA $OUTPUT_GEN/ca_cert.pem -CAkey $OUTPUT_GEN/ca_key.pem -in $OUTPUT_GEN/operator-cli_csr.pem -out $OUTPUT_GEN/operator-cli_cert.pem -days 365 -CAcreateserial -passin pass:$KEY_PASSWORD

## Sign operator certificate
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator.jks -alias selfsigned -certreq -file $OUTPUT_GEN/operator_csr.pem -storepass $KEYSTORE_PASSWORD
openssl x509 -extfile $OUTPUT_GEN/openssl.cnf -extensions extended -req -CA $OUTPUT_GEN/ca_cert.pem -CAkey $OUTPUT_GEN/ca_key.pem -in $OUTPUT_GEN/operator_csr.pem -out $OUTPUT_GEN/operator_cert.pem -days 365 -CAcreateserial -passin pass:$KEY_PASSWORD

## Import CA and operator cli signed certificate into operator cli keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $KEYSTORE_PASSWORD
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator-cli.jks -alias selfsigned -import -file $OUTPUT_GEN/operator-cli_cert.pem -storepass $KEYSTORE_PASSWORD

## Import CA and operator signed certificate into operator keystore
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $KEYSTORE_PASSWORD
keytool -noprompt -keystore $OUTPUT_GEN/keystore-operator.jks -alias selfsigned -import -file $OUTPUT_GEN/operator_cert.pem -storepass $KEYSTORE_PASSWORD

## Import CA into operator cli truststore
keytool -noprompt -keystore $OUTPUT_GEN/truststore-operator-cli.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $TRUSTSTORE_PASSWORD

## Import CA into operator truststore
keytool -noprompt -keystore $OUTPUT_GEN/truststore-operator.jks -alias CARoot -import -file $OUTPUT_GEN/ca_cert.pem -storepass $TRUSTSTORE_PASSWORD


