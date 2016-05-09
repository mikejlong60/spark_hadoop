#!/usr/bin/env bash

yum update -y
yum install java-1.8.0-openjdk-devel -y
yum install wget -y
wget http://www.scala-lang.org/files/archive/scala-2.10.6.tgz
tar xvf scala-2.10.6.tgz
mv scala-2.10.6 /usr/lib
adduser mjlong
su mjlong
ln -s /usr/lib/scala-2.10.6 /usr/lib/scala
export PATH=$PATH:/usr/lib/scala/bin

wget http://www-us.apache.org/dist/hadoop/common/hadoop-2.6.4/hadoop-2.6.4.tar.gz
wget http://www-eu.apache.org/dist/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.6.tgz
