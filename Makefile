all:
	git pull
	chmod +x popjrun-yarn
	vn install
	/popjrun-yarn -p popjava.jar -j app.jar -c 1 -M 1024 -C 10 -m ch.heia.pop.yarn.example.Main 1

