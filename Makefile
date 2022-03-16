define ANNOUNCE_BODY
Required section:
 build - build project into build directory, with configuration file and environment
 clean - clean all addition file, build directory and output archive file
endef

GENERATE_PROJECT_VERSION = $(mvn -q -Dexec.executable=echo -Dexec.args='${project.version}' --non-recursive exec:exec)
GENERATE_PROJECT_ARTIFACTID = $(mvn -q -Dexec.executable=echo -Dexec.args='${project.artifactId}' --non-recursive exec:exec)
GENERATE_PROJECT_GROUPID = $(mvn -q -Dexec.executable=echo -Dexec.args='${project.groupId}' --non-recursive exec:exec)
GENERATE_BRANCH = $(shell git name-rev $$(git rev-parse HEAD) | cut -d\  -f2 | sed -re 's/^(remotes\/)?origin\///' | tr '/' '_')

SET_PROJECT_VERSION = $(eval VERSION=$(GENERATE_PROJECT_VERSION))
SET_PROJECT_ARTIFACTID = $(eval PROJECT_NAME=$(GENERATE_PROJECT_ARTIFACTID))
SET_PROJECT_GROUPID = $(eval PROJECT_NAME=$(GENERATE_PROJECT_GROUPID))
SET_BRANCH = $(eval BRANCH=$(GENERATE_BRANCH))


.SILENT:

COMPONENTS :=

export ANNOUNCE_BODY
all:
	echo "$$ANNOUNCE_BODY"

build: clean package
	# required section
	echo Build
	$(GENERATE_PROJECT_GROUPID)
	$(GENERATE_PROJECT_ARTIFACTID)
	$(GENERATE_PROJECT_VERSION)
	mvn clean install -X

clean:
	# required section
	$(GENERATE_PROJECT_GROUPID)
	$(GENERATE_PROJECT_ARTIFACTID)
	$(GENERATE_PROJECT_VERSION)
	mvn clean
	rm -rf build $(GENERATE_PROJECT_ARTIFACTID)-*.tar.gz package target
