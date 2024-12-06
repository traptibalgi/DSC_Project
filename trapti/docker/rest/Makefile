VERSION=v6
DOCKERUSER=traptibalgi

build:
	docker build -f Dockerfile -t rest .

push:
	docker tag rest $(DOCKERUSER)/rest:$(VERSION)
	docker push $(DOCKERUSER)/rest:$(VERSION)
	docker tag demucs-rest $(DOCKERUSER)/rest:latest
	docker push $(DOCKERUSER)/rest:latest
