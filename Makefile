.DEFAULT_GOAL := help

clean:
	mvn clean

test:
	mvn test
	
install:
	mvn clean install
	
help:
	@ echo "Usage   :  make <target>"
	@ echo "Targets :"
	@ echo "   clean ......... Removes build products"	
	@ echo "   test .......... Builds and runs all unit tests"
	@ echo "   install ........Builds and installs to local repository"	
	@ echo "   help .......... Prints this help message"	
