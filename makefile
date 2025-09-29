PROJECT_ID ?= $(shell gcloud config get-value project)
IMAGE_REGION ?= europe
DEPLOY_REGION ?= europe-west1
REPOSITORY_NAME ?= main
SIMULATOR_SERVICE_NAME ?= ztbus-data-simulator
PROCESSOR_SERVICE_NAME ?= orca-python-processor

SIMULATOR_IMAGE_NAME = $(IMAGE_REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY_NAME)/$(SIMULATOR_SERVICE_NAME)
PROCESSOR_IMAGE_NAME = $(IMAGE_REGION)-docker.pkg.dev/$(PROJECT_ID)/$(REPOSITORY_NAME)/$(PROCESSOR_SERVICE_NAME)

.PHONY: build
build:
	docker build -t $(PROCESSOR_IMAGE_NAME) -f processor/dockerfile .
	docker build -t $(SIMULATOR_IMAGE_NAME) -f simulator/dockerfile .

.PHONY: push
push: build
	docker push $(PROCESSOR_IMAGE_NAME)
	docker push $(SIMULATOR_IMAGE_NAME)

.PHONY: deploy
deploy: push
	gcloud run deploy "$(SIMULATOR_SERVICE_NAME)" \
	    --image $(SIMULATOR_IMAGE_NAME) \
	    --platform managed \
	    --region $(DEPLOY_REGION) \
	    --update-secrets=ZTBUS_ADDR=ZTBUS_ADDR:latest \
	    --update-secrets=ZTBUS_DB=ZTBUS_DB:latest \
	    --update-secrets=ZTBUS_USER=ZTBUS_USER:latest \
	    --update-secrets=ZTBUS_PASS=ZTBUS_PASS:latest \
	    --update-secrets=ZTBUS_PORT=ZTBUS_PORT:latest \
	    --update-secrets=ORCA_CORE=ORCA_CORE:latest \
	    --update-secrets=PROCESSOR_EXTERNAL_PORT=PROCESSOR_EXTERNAL_PORT:latest \
	    --update-secrets=PROCESSOR_ADDRESS=PROCESSOR_ADDRESS:latest \
	    --update-secrets=ENV=ENV:latest \
	    --vpc-connector="projects/$(PROJECT_ID)/locations/$(DEPLOY_REGION)/connectors/orca-network-connector" \
	    --vpc-egress=all-traffic \
	    --ingress=internal \
	    --tag=production \
	    --port=8080 \
	    --allow-unauthenticated

	gcloud run deploy "$(PROCESSOR_SERVICE_NAME)" \
	    --image $(PROCESSOR_IMAGE_NAME) \
	    --platform managed \
	    --region $(DEPLOY_REGION) \
	    --update-secrets=ZTBUS_ADDR=ZTBUS_ADDR:latest \
	    --update-secrets=ZTBUS_DB=ZTBUS_DB:latest \
	    --update-secrets=ZTBUS_USER=ZTBUS_USER:latest \
	    --update-secrets=ZTBUS_PASS=ZTBUS_PASS:latest \
	    --update-secrets=ZTBUS_PORT=ZTBUS_PORT:latest \
	    --update-secrets=ORCA_CORE=ORCA_CORE:latest \
	    --update-secrets=PROCESSOR_EXTERNAL_PORT=PROCESSOR_EXTERNAL_PORT:latest \
	    --update-secrets=PROCESSOR_ADDRESS=PROCESSOR_ADDRESS:latest \
	    --update-secrets=ENV=ENV:latest \
	    --vpc-connector="projects/$(PROJECT_ID)/locations/$(DEPLOY_REGION)/connectors/orca-network-connector" \
	    --vpc-egress=all-traffic \
	    --ingress=internal \
	    --tag=production \
	    --port=8080 \
	    --allow-unauthenticated
