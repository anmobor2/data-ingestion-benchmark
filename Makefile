
KIND_NAMESPACE := myc-ingestion-test
KIND_CLUSTER := myc-ingestion-benchmark
VERSION := 1.0
DOCKER_IMAGE_NAME := pyingestiontoolkit
CONTAINER_REGISTRY := 224392328862.dkr.ecr.eu-west-1.amazonaws.com

psrecorder: build-pytoolkit kind-load kind-apply kind-restart


ecr-login:
	aws ecr get-login-password --region eu-west-1 | docker login --username AWS --password-stdin 224392328862.dkr.ecr.eu-west-1.amazonaws.com

build-pytoolkit:
	docker build \
		-f deployment/docker/pyingestiontoolkit.dockerfile \
		-t $(DOCKER_IMAGE_NAME):$(VERSION) \
		.

push-pytoolkit: ecr-login
	docker tag $(DOCKER_IMAGE_NAME):$(VERSION) $(CONTAINER_REGISTRY)/$(DOCKER_IMAGE_NAME):$(VERSION)
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

psrecorder-secrets:
	kubectl create secret generic psrecorder-secrets --from-env-file=psrecorder.env \
		--save-config --dry-run=client \
 		-o yaml | kubectl apply -f -

psrecorder-show-secrets:
	kubectl get secret psrecorder-secrets -o json | jq '.data | map_values(@base64d)'

psrecorder-apply:
	kubectl apply -f deployment/k8s/base/psrecorder/psrecorder.yaml

psrecorder-restart:
	kubectl rollout restart deployment psrecorder

psrecorder-logs:
	kubectl logs -l app=psrecorder --all-containers=true -f --tail=100


kind-status:
	kubectl get pods -o wide --watch

kind-pods:
	kubectl get pods -o wide


promscale-apply:
	kubectl apply -f deployment/k8s/base/promscale/promscale.yaml

grafana-apply:
	kubectl apply -f deployment/k8s/base/grafana/grafana.yaml

vernemq-apply:
	kubectl apply -f deployment/k8s/base/vernemq/vernemq.yaml

vernemq-delete:
	kubectl delete -f deployment/k8s/base/vernemq/vernemq.yaml

verne-logs:
	kubectl logs -l app=vernemq --all-containers=true -f --tail=100

verne2prom-apply:
	kubectl apply -f deployment/k8s/base/verne2promscale/verne2promscale.yaml

verne2prom-delete:
	kubectl delete -f deployment/k8s/base/verne2promscale/verne2promscale.yaml

verne2prom-logs:
	kubectl logs -l app=verne2promscale --all-containers=true -f --tail=100

verne2prom-restart:
	kubectl rollout restart deployment verne2promscale

promscale-status:
	kubectl get pods -o wide --watch