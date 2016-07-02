all:
	git pull
	chmod +x popjrun-yarn
	mvn install
	./popjrun-yarn -p popjava.jar -j app.jar -t 1 -M 1024 -c 10 -m ch.heia.pop.yarn.example.Main 1

