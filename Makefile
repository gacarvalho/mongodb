DOCKER_NETWORK = hadoop-network
ENV_FILE = hadoop.env
VERSION_REPOSITORY_DOCKER = 1.0.1

current_branch := $(shell git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "default-branch")

# Realiza criacao da REDE DOCKER_NETWORK
create-network:
	docker network create hadoop_network

  # ################## 1.2 #####################
  # APP { INGESTAO DO MONGO DB PARA O HDFS }
  # ############################################

build-app-ingestion-reviews-mongodb-hdfs-compass:
	docker build -t iamgacarvalho/dmc-app-ingestion-reviews-mongodb-hdfs-compass:$(VERSION_REPOSITORY_DOCKER) .
	docker push iamgacarvalho/dmc-app-ingestion-reviews-mongodb-hdfs-compass:$(VERSION_REPOSITORY_DOCKER)

restart-docker:
	sudo systemctl restart docker

down-services:
	docker rm -f $(docker ps -a -q)

