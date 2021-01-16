# Flink demo

Example of Flink jobs based on DataStream API.

## Compile and execute job

Install Java 11 and Maven 3. We recommend installing Adopt OpenJDK 11.

Configure the path of the Java JDK:

    export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home

Compile the code with Maven:

    mvn clean compile

Package the code including the Flink runtime:

    mvn clean package -DincludeFlinkRuntime=true -DskipTests=true

Create the configuration file computeaverage.conf:

    rest-port: 8081
    source-delay-array: 250 10
    source-delay-interval: 300000
    source-limit: 0
    console-verbosity: 1
    job-name: computeaverage
    disable-chaining: true
    checkpoint-interval: 600000
    window-size: 60000
    window-slide: 10000
    max-out-of-orderness: 5000
    bucket-check-interval: 30000
    bucket-rollover-interval: 300000
    bucket-inactivity-interval: 300000
    bucket-output-path: /computeaverage/output
    partitions: 32

Define the job name:

    export JOB_NAME=computeaverage

Define the job class:

    export JOB_CLASS=com.nextbreakpoint.flink.jobs.ComputeAverage

Execute the job with Maven:

    mvn exec:java -DexecuteJob=true -DclassName=$JOB_CLASS -Dexec.args="--JOB_PARAMETERS file://$(pwd)/config/${JOB_NAME}.conf --OUPUT_LOCATION file://$(pwd)/tmp/output"

Or alternatively use java command:

    export CLASSPATH=$(find "target/libs" -name '*.jar' | xargs echo | tr ' ' ':')

    java -classpath target/demo-1.0.0-shaded.jar:${CLASSPATH} -Denable.developer.mode="true" -Dstate.savepoints.dir=file://$(pwd)/tmp/savepoints -Dstate.checkpoints.dir=file://$(pwd)/tmp/checkpoints ${JOB_CLASS} --JOB_PARAMETERS file://$(pwd)/config/${JOB_NAME}.conf --OUPUT_LOCATION file://$(pwd)/tmp/output

## Execute job on standalone server

Install Minikube and jq. We recommend Minikube 1.11.0.

Start Minikube with at least 8Gb of memory and mount the config directory:

    minikube start --cpus=2 --memory=8gb --kubernetes-version v1.18.14 --mount-string="$(pwd)/config:/var/config" --mount

Configure the path of the Java JDK:

    export JAVA_HOME=/Library/Java/JavaVirtualMachines/adoptopenjdk-11.jdk/Contents/Home

Package the code without Flink runtime (since the job will run on the standalone server):

    mvn clean package -DincludeFlinkRuntime=false -DskipTests=true

Configure Docker:

    eval $(minikube docker-env)

Define the S3 bucket name:

    export S3_BUCKET=nextbreakpoint-demo

Define the S3 configuration (optional):

    export S3_ACCESS_KEY=<your_access_key>
    export S3_SECRET_KEY=<your_secret_key>
    export S3_PATH_STYLE_ACCESS="false"
    export S3_ENDPOINT=https://s3.<your_zone>.amazonaws.com

Or use the default configuration:

    export S3_ACCESS_KEY=minioaccesskey
    export S3_SECRET_KEY=miniosecretkey
    export S3_PATH_STYLE_ACCESS="true"
    export S3_ENDPOINT=http://minio:9000

Start services using Docker compose:

    docker-compose up -d

Create the S3 bucket:

    export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY}
    export AWS_SECRET_ACCESS_KEY=${S3_SECRET_KEY}
    aws --endpoint-url=http://$(minikube ip):9000 s3 mb s3://${S3_BUCKET}

Define the Flink host:

    export FLINK_HOST=$(minikube ip)

Define the job name:

    export JOB_NAME=computeaverage

Define the job class:

    export JOB_CLASS=com.nextbreakpoint.flink.jobs.ComputeAverage

Define the job arguments (only if it doesn't exist already):

    export JOB_ARGUMENTS="--JOB_PARAMETERS file:///var/config/${JOB_NAME}.conf --OUPUT_LOCATION s3a://${S3_BUCKET}"

Upload the JAR and run the job:

    curl -X POST -H "Expect:" -F "jarfile=@target/demo-1.0.0-shaded.jar" http://${FLINK_HOST}:8081/jars/upload
    JARID=$(curl -s http://${FLINK_HOST}:8081/jars | jq -r '.files[0].id')
    curl -X POST -d "{\"entryClass\":\"${JOB_CLASS}\",\"parallelism\":\"2\",\"programArgs\":\"${JOB_ARGUMENTS}\"}" http://${FLINK_HOST}:8081/jars/${JARID}/run

Or upload the JAR and run the job from a savepoint:

    curl -X POST -H "Expect:" -F "jarfile=@target/demo-1.0.0-shaded.jar" http://${FLINK_HOST}:8081/jars/upload
    JARID=$(curl -s http://${FLINK_HOST}:8081/jars | jq -r '.files[0].id')
    curl -X POST -d "{\"entryClass\":\"${JOB_CLASS}\",\"parallelism\":\"2\",\"programArgs\":\"${JOB_ARGUMENTS}\",\"savepointPath\":\"${SAVEPOINT_PATH}\"}" http://${FLINK_HOST}:8081/jars/${JARID}/run

Inspect the job using the Flink's web console:

    http://${FLINK_HOST}:8081

Stop the job and create a savepoint:

    JOB=$(curl -s http://${FLINK_HOST}:8081/jobs | jq -r '.jobs[0] | select(.status=="RUNNING") | .id')
    curl -X POST -H 'content-type: application/json' http://${FLINK_HOST}:8081/jobs/$JOB/stop -d "{\"drain\": false, \"targetDirectory\": \"s3p://${S3_BUCKET}/${JOB_NAME}/savepoints\"}"

Or cancel the job without savepoint:

    JOB=$(curl -s http://${FLINK_HOST}:8081/jobs | jq -r '.jobs[0] | select(.status=="RUNNING") | .id')
    curl -X PATCH http://${FLINK_HOST}:8081/jobs/$JOB

Stop all services:

    docker-compose down
