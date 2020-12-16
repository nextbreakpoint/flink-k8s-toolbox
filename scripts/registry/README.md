# Install a local Docker Registry

Create docker-registry files:

    ./docker-registry-setup.sh

Create docker-registry:

    kubectl create -f docker-registry.yaml

Add this entry to your hosts file (etc/hosts):

    127.0.0.1 registry

Create pull secrets in flink namespace:

    kubectl create secret docker-registry regcred -n flink --docker-server=registry:30000 --docker-username=test --docker-password=password --docker-email=<your-email>

Associate pull secrets to flink-operator service account:

    kubectl patch serviceaccount flink-operator -n flink -p '{"imagePullSecrets": [{"name": "regcred"}]}'

You can tag and push images to your local registry:

    docker tag flink:1.9.2 registry:30000/flink:1.9.2
    docker login registry:30000
    docker push registry:30000/flink:1.9.2
