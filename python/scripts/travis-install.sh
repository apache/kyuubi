#!/bin/bash -eux

source /etc/lsb-release

echo "deb [arch=amd64] https://archive.cloudera.com/${CDH}/ubuntu/${DISTRIB_CODENAME}/amd64/cdh ${DISTRIB_CODENAME}-cdh${CDH_VERSION} contrib
deb-src https://archive.cloudera.com/${CDH}/ubuntu/${DISTRIB_CODENAME}/amd64/cdh ${DISTRIB_CODENAME}-cdh${CDH_VERSION} contrib" | sudo tee /etc/apt/sources.list.d/cloudera.list
sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 327574EE02A818DD
sudo apt-get -q update

sudo apt-get -q install -y oracle-java8-installer python-dev g++ libsasl2-dev maven
sudo update-java-alternatives -s java-8-oracle

#
# LDAP
#
sudo apt-get -q -y --no-install-suggests --no-install-recommends --force-yes install ldap-utils slapd
sudo mkdir -p /tmp/slapd
sudo slapd -f $(dirname $0)/ldap_config/slapd.conf -h ldap://localhost:3389 &
while ! nc -vz localhost 3389; do sleep 1; done
sudo ldapadd -h localhost:3389 -D cn=admin,dc=example,dc=com -w test -f $(dirname $0)/../pyhive/tests/ldif_data/base.ldif
sudo ldapadd -h localhost:3389 -D cn=admin,dc=example,dc=com -w test -f $(dirname $0)/../pyhive/tests/ldif_data/INITIAL_TESTDATA.ldif

#
# Hive
#

sudo apt-get -q install -y --force-yes hive

javac -cp /usr/lib/hive/lib/hive-service.jar $(dirname $0)/travis-conf/com/dropbox/DummyPasswdAuthenticationProvider.java
jar cf $(dirname $0)/dummy-auth.jar -C $(dirname $0)/travis-conf com
sudo cp $(dirname $0)/dummy-auth.jar /usr/lib/hive/lib

# Hack around broken symlink in Hive's installation
# /usr/lib/hive/lib/zookeeper.jar -> ../../zookeeper/zookeeper.jar
# Without this, Hive fails to start up due to failing to find ZK classes.
sudo ln -nsfv /usr/share/java/zookeeper.jar /usr/lib/hive/lib/zookeeper.jar

sudo mkdir -p /user/hive
sudo chown hive:hive /user/hive
sudo cp $(dirname $0)/travis-conf/hive/hive-site.xml /etc/hive/conf/hive-site.xml
sudo apt-get -q install -y --force-yes hive-metastore hive-server2 || (grep . /var/log/hive/* && exit 2)

while ! nc -vz localhost 9083; do sleep 1; done
while ! nc -vz localhost 10000; do sleep 1; done

sudo -Eu hive $(dirname $0)/make_test_tables.sh

#
# Presto
#

sudo apt-get -q install -y python # Use python2 for presto server

mvn -q org.apache.maven.plugins:maven-dependency-plugin:3.0.0:copy \
    -Dartifact=com.facebook.presto:presto-server:${PRESTO}:tar.gz \
    -DoutputDirectory=.
tar -x -z -f presto-server-*.tar.gz
rm -rf presto-server
mv presto-server-*/ presto-server

cp -r $(dirname $0)/travis-conf/presto presto-server/etc

/usr/bin/python2.7 presto-server/bin/launcher.py start

#
# Trino
#

sudo apt-get -q install -y python # Use python2 for trino server

mvn -q org.apache.maven.plugins:maven-dependency-plugin:3.0.0:copy \
    -Dartifact=io.trino:trino-server:${TRINO}:tar.gz \
    -DoutputDirectory=.
tar -x -z -f trino-server-*.tar.gz
rm -rf trino-server
mv trino-server-*/ trino-server

cp -r $(dirname $0)/travis-conf/trino trino-server/etc

/usr/bin/python2.7 trino-server/bin/launcher.py start

#
# Python
#

pip install $SQLALCHEMY
pip install -e .
pip install -r dev_requirements.txt

# Sleep so Presto has time to start up.
# Otherwise we might get 'No nodes available to run query' or 'Presto server is still initializing'
while ! grep -q 'SERVER STARTED' /tmp/presto/data/var/log/server.log; do sleep 1; done

# Sleep so Trino has time to start up.
# Otherwise we might get 'No nodes available to run query' or 'Presto server is still initializing'
while ! grep -q 'SERVER STARTED' /tmp/trino/data/var/log/server.log; do sleep 1; done
