# Storm

## Commands
#### Create docker instances, copy output jar to nimbus and initiate topology
docker-compose -f stack.yml up -d

docker cp target/storm-example.jar nimbus:/opt/

docker exec -it nimbus storm jar /opt/storm-example.jar pl.edu.storm.StormExampleApp

#### Read logs from workers (either on supervisor or superviser-2)
docker exec -it supervisor-2 bash

docker exec -it supervisor bash

cat /logs/workers-artifacts/adzd-topology-2-1607338059/6700/worker.log 
