
KIND_NAMESPACE := myc-ingestion-test
KIND_CLUSTER := myc-ingestion-benchmark
VERSION := 1.0
DOCKER_IMAGE_NAME := psrecorder
CONTAINER_REGISTRY := 224392328862.dkr.ecr.eu-west-1.amazonaws.com

psrecorder: build-psrecorder kind-load kind-apply kind-restart


build-psrecorder:
	docker build \
		-f deployment/docker/psrecorder.dockerfile \
		-t $(DOCKER_IMAGE_NAME):$(VERSION) \
		.

push-psrecorder:
	docker tag $(CONTAINER_REGISTRY)/$(DOCKER_IMAGE_NAME):$(VERSION)
	docker push $(CONTAINER_REGISTRY)/$(DOCKER_IMAGE_NAME):$(VERSION)

kind-up:
	kind create cluster \
		--image kindest/node:v1.21.1@sha256:69860bda5563ac81e3c0057d654b5253219618a22ec3a346306239bba8cfa1a6 \
		--name $(KIND_CLUSTER) \
		--config deployment/k8s/kind/kind-config.yaml
	kubectl create namespace $(KIND_NAMESPACE)
	kubectl config set-context --current --namespace=myc-ingestion-test

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)

kind-load:
	kind load docker-image $(DOCKER_IMAGE_NAME):$(VERSION) --name $(KIND_CLUSTER)

kind-secrets:
	kubectl create secret generic psrecorder-secrets --from-env-file=.env \
		--save-config --dry-run=client \
 		-o yaml | kubectl apply -f -

kind-show-secrets:
	kubectl get secret psrecorder-secrets -o json | jq '.data | map_values(@base64d)'

kind-apply:
	kubectl apply -f deployment/k8s/base/psrecorder-pod/psrecorder.yaml

kind-status:
	kubectl get pods -o wide --watch

kind-restart:
	kubectl rollout restart deployment psrecorder


kind-logs:
	kubectl logs -l app=psrecorder --all-containers=true -f --tail=100