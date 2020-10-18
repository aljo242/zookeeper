ECHO Shutting down Docker Zookeeper
docker stack rm zookeeper
docker swarm leave --force
