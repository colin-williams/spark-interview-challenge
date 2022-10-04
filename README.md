# secure-spark-demo

* Calculates the "TopN" sources and resource paths for a provided dataset

## Documentation

Development used ```JDK 11``` and ```Scala 2.12``` and ```Sbt 1.4.9``` and ```Spark 3.1.x```

### Tests
* tests can be run from the root directory using ```sbt test```  OR invoked by intelliJ idea. 


* To invoke tests in intelliJ IDEA:
The Scala plugin should be installed, we should set a JDK 11 distrubution, and Include dependencies with provided scope 
should be set in run configuration 

### Assembly

* an assembly jar can be created using ```sbt assembly``` that can be run on a spark cluster using ```spark-submit```

### Docker

* a ```Dockerfile``` is provided in the ```spark-docker``` directory. Run the shell script ```build-docker.sh``` to 
build a docker image called ```spark-test```.


* After generating the docker image it can be run with `docker run spark-test`. The image will download the dataset and
execute the assembly jar using `spark-submit`. The dataframes are previewed using show() and written to a file inside
the container during execution. 



## Author

* @author: colin.williams.seattle@gmail.com
* OSS licenses have been retained respectively where applicable
