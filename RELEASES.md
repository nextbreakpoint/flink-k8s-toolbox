# 1.4.4-beta

Release notes:
- Update example to Flink 1.12.
- Add property affinity to JobManager and TaskManager spec. 
- Add property tolerations to JobManager and TaskManager spec.
- Add property topologySpreadConstraints to JobManager and TaskManager spec.

# 1.4.3-beta

Release notes:
- Add properties command and args to JobManager and TaskManager spec.
- Fix scaling issues (supervisor doesn't detect idle taskmanagers).
- Fix issue in restart cluster on resource changed.  
- Update example to Flink 1.11.
- Stop supporting old versions of Flink.

# 1.4.2-beta

Release notes:
- Configurable operator's replicas to enable HA.
- Configurable supervisor's replicas to enable HA.
- Accept zero as parallelism value to suspend job.
- Replace watch with informer.
- Stability improvements.    

# 1.4.1-beta

Release notes:
- Ensure that cache is fully populate before reconciling resources.

# 1.4.0-beta

Release notes:
- Support for new custom resources: FlinkDeployment, FlinkCluster, FlinkJob.
- New toolbox distribution which includes native command flinkctl.
- New quick start tutorial with example of Flink deployment.
- New manual with detailed instructions.
- Many code improvements.
- Compiled using GraalVM and Native Image.
