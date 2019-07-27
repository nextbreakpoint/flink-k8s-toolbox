#!/bin/bash

if [ -z "$FLINK_SAVEPOINTS_LOCATION" ]; then
  echo "Flink savepoints location not defined. Will use default value"
fi

if [ -z "$FLINK_CHECKPOINTS_LOCATION" ]; then
  echo "Flink checkpoints location not defined. Will use default value"
fi

if [ -z "$FLINK_FS_CHECKPOINTS_LOCATION" ]; then
  echo "Flink FS checkpoints location not defined. Will use default value"
fi

if [ -z "$FLINK_GRAPHITE_HOST" ]; then
  echo "Graphite host not defined. Will use default value (graphite)"
  export FLINK_GRAPHITE_HOST=graphite
fi

if [ -z "$FLINK_GRAPHITE_PORT" ]; then
  echo "Graphite port not defined. Will use default value (2003)"
  export FLINK_GRAPHITE_PORT=2003
fi

if [ -z "$FLINK_GRAPHITE_PREFIX" ]; then
  echo "Graphite prefix not defined. Will use default value"
  export FLINK_GRAPHITE_PREFIX="flink"
fi

# If unspecified, the hostname of the container is taken as the JobManager address
JOB_MANAGER_RPC_ADDRESS=${JOB_MANAGER_RPC_ADDRESS:-$(hostname -f)}

if [ "$1" = "help" ]; then
    echo "Usage: $(basename "$0") (jobmanager|taskmanager|local|help)"
    exit 0
elif [ "$1" = "jobmanager" ]; then
    echo "Starting Job Manager..."

    echo "metrics.reporters: graphite" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.class: org.apache.flink.metrics.graphite.GraphiteReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.host: $FLINK_GRAPHITE_HOST" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.port: $FLINK_GRAPHITE_PORT" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.prefix: $FLINK_GRAPHITE_PREFIX" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.protocol: TCP" >> "$FLINK_HOME/conf/flink-conf.yaml"

    if [ -n "$FLINK_SAVEPOINTS_LOCATION" ]; then
      echo "state.savepoints.dir: $FLINK_SAVEPOINTS_LOCATION" >> "$FLINK_PATH/conf/flink-conf.yaml"
    fi

    if [ -n "$FLINK_CHECKPOINTS_LOCATION" ]; then
      echo "state.checkpoints.dir: $FLINK_CHECKPOINTS_LOCATION" >> "$FLINK_PATH/conf/flink-conf.yaml"
    fi

    if [ -n "$FLINK_FS_CHECKPOINTS_LOCATION" ]; then
      echo "state.backend.fs.checkpointdir: $FLINK_FS_CHECKPOINTS_LOCATION" >> "$FLINK_PATH/conf/flink-conf.yaml"
    fi

    echo "jobmanager.rpc.address: ${JOB_MANAGER_RPC_ADDRESS}" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "blob.server.port: 6124" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "query.server.port: 6125" >> "$FLINK_HOME/conf/flink-conf.yaml"

    echo "config file: " && grep '^[^\n#]' "$FLINK_HOME/conf/flink-conf.yaml"
    bash "$FLINK_HOME/bin/jobmanager.sh" start-foreground ${JOB_MANAGER_RPC_ADDRESS}
    exit 0
elif [ "$1" = "taskmanager" ]; then
    if [ -z "$TASK_MANAGER_NUMBER_OF_TASK_SLOTS" ]; then
      export TASK_MANAGER_NUMBER_OF_TASK_SLOTS=${TASK_MANAGER_NUMBER_OF_TASK_SLOTS:-$(grep -c ^processor /proc/cpuinfo)}
    fi

    echo "jobmanager.rpc.address: ${JOB_MANAGER_RPC_ADDRESS}" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "taskmanager.numberOfTaskSlots: $TASK_MANAGER_NUMBER_OF_TASK_SLOTS" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "blob.server.port: 6124" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "query.server.port: 6125" >> "$FLINK_HOME/conf/flink-conf.yaml"

    echo "Starting Task Manager..."

    echo "metrics.reporters: graphite" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.class: org.apache.flink.metrics.graphite.GraphiteReporter" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.host: $FLINK_GRAPHITE_HOST" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.port: $FLINK_GRAPHITE_PORT" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.prefix: $FLINK_GRAPHITE_PREFIX" >> "$FLINK_HOME/conf/flink-conf.yaml"
    echo "metrics.reporter.graphite.protocol: TCP" >> "$FLINK_HOME/conf/flink-conf.yaml"

    if [ -n "$FLINK_SAVEPOINTS_LOCATION" ]; then
      echo "state.savepoints.dir: $FLINK_SAVEPOINTS_LOCATION" >> "$FLINK_PATH/conf/flink-conf.yaml"
    fi

    if [ -n "$FLINK_CHECKPOINTS_LOCATION" ]; then
      echo "state.checkpoints.dir: $FLINK_CHECKPOINTS_LOCATION" >> "$FLINK_PATH/conf/flink-conf.yaml"
    fi

    if [ -n "$FLINK_FS_CHECKPOINTS_LOCATION" ]; then
      echo "state.backend.fs.checkpointdir: $FLINK_FS_CHECKPOINTS_LOCATION" >> "$FLINK_PATH/conf/flink-conf.yaml"
    fi

    echo "config file: " && grep '^[^\n#]' "$FLINK_HOME/conf/flink-conf.yaml"
    bash "$FLINK_HOME/bin/taskmanager.sh" start-foreground
    exit 0
elif [ "$1" = "local" ]; then
    echo "Starting local cluster..."
    bash "$FLINK_HOME/bin/jobmanager.sh" start-foreground local
    exit 0
else
    exec "$@"
fi
