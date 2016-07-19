# pop-java-yarn
Porting POP-Java to YARN

This project contains two sub-projects, an extension to the original pop-java and an example project which can be used to create popjava application using maven. The example project is especially meant to be used with YARN but should work with any program.

POP-Java doesn't currently have a online maven repository, it's necessary to install it manually in the local.
`$ cd pop-java`

`$ ant`

`$ mvn install:install-file -Dfile=build/jar/popjava.jar -DgroupId=popjava -DartifactId=popjava -Dversion=1.0 -Dpackaging=jar`

`$ cd ../pop-java-yarn`

`$ mvn install`

This procedure will compile the standard popjava and make it available to other projects.
