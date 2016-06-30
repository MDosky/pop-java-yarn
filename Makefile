all:
	mvn assembly:assembly -DdescriptorId=jar-with-dependencies
	mv target/simple-yarn-app-1.1.0-jar-with-dependencies.jar simpleapp.jar
	./popjrun-yarn -j simpleapp.jar -c 1 -M 1024 -C 10 -m ch.heia.popdna.myapp.DoSomething
