define ANNOUNCE_BODY
Required section:
 build - build project into build directory, with configuration file and environment
 clean - clean all addition file, build directory and output archive file
 test - run all tests
 pack - make output archive
endef


GENERATE_VERSION = $(shell mvn -q -Dexec.executable=echo -Dexec.args='$${project.version}' --non-recursive exec:exec 2>/dev/null)
GENERATE_BRANCH = $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')
GENERATE_PROJECT_NAME = $(shell mvn -q -Dexec.executable=echo -Dexec.args='$${project.artifactId}' --non-recursive exec:exec 2>/dev/null)
GENERATE_PROJECT_NAME_LOW_CASE = $(shell mvn -q -Dexec.executable=echo -Dexec.args='$${project.artifactId}' --non-recursive exec:exec 2>/dev/null | sed -e 's/\(.*\)/\L\1/')


SET_VERSION = $(eval VERSION=$(GENERATE_VERSION))
SET_BRANCH = $(eval BRANCH=$(GENERATE_BRANCH))
SET_PROJECT_NAME = $(eval PROJECT_NAME=$(GENERATE_PROJECT_NAME))
SET_PROJECT_NAME_LOW_CASE = $(eval PROJECT_NAME_LOW_CASE=$(GENERATE_PROJECT_NAME_LOW_CASE))

.SILENT:

export ANNOUNCE_BODY
all:
	echo "$$ANNOUNCE_BODY"

pack: build
	$(SET_VERSION)
	$(SET_BRANCH)
	$(SET_PROJECT_NAME_LOW_CASE)
	rm -f $(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz
	echo Create archive \"$(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz\"
	cd build; tar czf ../$(PROJECT_NAME_LOW_CASE)-$(VERSION)-$(BRANCH).tar.gz .

build:
	# required section
	echo Build
	$(SET_VERSION)
	$(SET_PROJECT_NAME)
	$(SET_PROJECT_NAME_LOW_CASE)
	mvn clean install
	mkdir build
	mkdir build/$(PROJECT_NAME)
	cp $(PROJECT_NAME)-nar/target/$(PROJECT_NAME)-nar-$(VERSION).nar  build/$(PROJECT_NAME)/
	cp README.md build/$(PROJECT_NAME)/
	cp CHANGELOG.md build/$(PROJECT_NAME)/
	cp LICENSE.md build/$(PROJECT_NAME)/
	
clean:
	# required section
	$(SET_VERSION)
	$(SET_PROJECT_NAME_LOW_CASE)
	mvn clean
	rm -rf build $(PROJECT_NAME_LOW_CASE)-*.tar.gz package project target

test:
	# required section
	echo "Testing..."	