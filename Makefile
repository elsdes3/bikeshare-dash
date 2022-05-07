#################################################################################
# GLOBALS                                                                       #
#################################################################################

# .EXPORT_ALL_VARIABLES:
# ANSIBLE_LOCALHOST_WARNING=false

#################################################################################
# COMMANDS                                                                      #
#################################################################################

## Run lint checks manually
lint:
	@echo "+ $@"
	@if [ ! -d .git ]; then git init && git add .; fi;
	@tox -e lint
.PHONY: lint

## Remove Python artifacts
clean-py:
	@echo "+ $@"
	@find . -type f -name "*.py[co]" -delete
	@find . -type d -name "__pycache__" -delete
.PHONY: clean-py

## Create AWS resources
aws-create:
	@echo "+ $@"
	@tox -e aws -- "aws-create"
.PHONY: aws-create

## Configure Prefect Cloud and Run Jupyterlab
build:
	@echo "+ $@"
	@tox -e build -- --prefect-storage-config-name "madeup-s3-storage21" --tags "configure"
.PHONY: build

## Run dashboard v2 application
dash-v2:
	@echo "+ $@"
	@tox -e dashv2
.PHONY: dash-v2

## Run data pipeline to create resources
pipe-create:
	@echo "+ $@"
	@tox -e pipe -- --action "create"
.PHONY: pipe-create

## Run data pipeline to delete resources
pipe-delete:
	@echo "+ $@"
	@tox -e pipe -- --action "delete"
.PHONY: pipe-delete

## Run CI build
ci:
	@echo "+ $@"
	@tox -e ci -- --prefect-storage-config-name "madeup-s3-storage22" --tags "configure"
.PHONY: ci

## Delete AWS resources
aws-delete:
	@echo "+ $@"
	@tox -e aws -- "aws-delete"
.PHONY: aws-delete

## Convert notebooks to HTML
nb-convert:
	@echo "+ $@"
	@tox -e nbconvert -- "executed_notebooks"
.PHONY: nb-convert

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################



#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
