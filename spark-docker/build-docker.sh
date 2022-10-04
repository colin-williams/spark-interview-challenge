#!/bin/bash
cd ../
sbt assembly
cd ./spark-docker
cp ../target/scala-2.12/secureworks-spark-demo-assembly-0.0.1.jar ./
docker build -t spark-test .
