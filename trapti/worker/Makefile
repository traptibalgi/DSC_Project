VERSION=v3
DOCKERUSER=traptibalgi

build:
	docker build -f Dockerfile -t proj-worker .
push:
	docker tag proj-worker $(DOCKERUSER)/proj-worker:$(VERSION)
	docker push $(DOCKERUSER)/proj-worker:$(VERSION)
	docker tag proj-worker $(DOCKERUSER)/proj-worker:latest
	docker push $(DOCKERUSER)/proj-worker:latest
