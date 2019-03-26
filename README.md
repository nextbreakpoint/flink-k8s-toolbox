# flink-submit

FlinkSubmit is a simple CLI utility for managing Flink clusters on Kubernetes.

## How to build

Build the application using Maven:

    mvn clean package

## How to use

List available commands with the command: 

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar --help

The command will produce the output:
  
    Usage: FlinkSubmit [OPTIONS] COMMAND [ARGS]...
    
    Options:
      -h, --help  Show this message and exit
    
    Commands:
      create  Create a cluster
      delete  Delete a cluster
      submit  Submit a job
      cancel  Cancel a job
      list    List jobs

## How to create a cluster

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar / 
        create /
        --kube-config=some-kubectl.conf /
        --cluster-name=my-flink-cluster /
        --environment=test /
        --image=docker-repo/image-name:image-version /
        --image-pull-secrets=secrets-name     

Show all parameters with the command: 

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar create --help

## How to delete a cluster

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar / 
        delete /
        --kube-config=some-kubectl.conf /
        --cluster-name=my-flink-cluster /
        --environment=test

Show all parameters with the command: 

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar delete --help

## How to submit a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar / 
        submit /
        --kube-config=some-kubectl.conf /
        --cluster-name=my-flink-cluster /
        --environment=test /
        --class-name=your-class /
        --jar-path=your-jar /
        --arguments="--input=...,--output=..."

Show all parameters with the command: 

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar submit --help

## How to list jobs

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar / 
        list /
        --kube-config=some-kubectl.conf /
        --cluster-name=my-flink-cluster /
        --environment=test

Show all parameters with the command: 

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar list --help

## How to cancel a job

Execute the command:

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar / 
        cancel /
        --kube-config=some-kubectl.conf /
        --cluster-name=my-flink-cluster /
        --environment=test /
        --create-savepoint /
        --job-id=your-job-id

Show all parameters with the command: 

    java -jar target/com.nextbreakpoint.flinksubmit-1.0.0.jar cancel --help
