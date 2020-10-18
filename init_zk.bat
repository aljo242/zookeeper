ECHO Starting Zookeeper in Replicated Mode
docker swarm init
docker stack deploy --compose-file stack.yml zookeeper