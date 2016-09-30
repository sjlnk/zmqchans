#! /usr/bin/env sh

## Tested to work with f831846 and on x86_64 linux.
## Some of the code here is platform specific, adjust to you own platform.
## Run at your own risk, sanity check the script for your own system!
## This will give you an idea how to build/install/deploy...

## Immediately exit on error.
set -e

## Customize these:
ROOT="$HOME/tmp/jzmqbuild"
LIBZMQ_ROOT="$HOME/opt/libzmq/4.1.5"
LIBZMQ_VERSION="4.1.5"
JZMQ_ROOT="$ROOT/jmzq"
GROUPID="org.zmapi"
MVN_REPOSITORY=$(echo "$HOME/.m2/repository/"$(echo $GROUPID | sed 's/\./\//g;'))
DEPLOY_TARGET_DIR=$JZMQ_ROOT/target_deploy

ResetGitRepo () {
  git checkout .
  find . -name ".gitignore" -exec rm -v {} \;
  git clean -fd
}

CreateNativeJar () {
  local old_pwd=$PWD

  local nat_bundle_dir=$DEPLOY_TARGET_DIR
  mkdir -p $nat_bundle_dir
  cp -v $JZMQ_ROOT/jzmq-jni/target/*native*.jar $nat_bundle_dir
  cd $nat_bundle_dir

  local jar_nat_path=$(dirname $(unzip -l $(ls *.jar) \
                 | grep "NATIVE.*jzmq" | awk '{print $NF}'))

  mkdir -p $jar_nat_path
  cp -Lv $LIBZMQ_ROOT/lib/libzmq.so $jar_nat_path
  local jar_nat_path_first=`echo $jar_nat_path | cut -d'/' -f1`
  echo $jar_nat_path_first

  zip -ur `ls *.jar*` $jar_nat_path_first
  rm -rv $jar_nat_path_first

  cd $old_pwd
}

mkdir -p $DEPLOY_TARGET_DIR

## Check that LIBZMQ_ROOT exists
file $LIBZMQ_ROOT

CPATH="$LIBZMQ_ROOT/include:$CPATH"
LD_LIBRARY_PATH="$LIBZMQ_ROOT/lib:$LD_LIBRARY_PATH"

mkdir -p $ROOT
cd $ROOT

if [ ! -d "$JZMQ_ROOT" ]; then
  git clone https://github.com/zeromq/jzmq.git $JZMQ_ROOT
fi

cd $JZMQ_ROOT

ResetGitRepo

cd jzmq-jni

./autogen.sh
./configure
make

cd $JZMQ_ROOT

VERSION="`git rev-parse HEAD | cut -c 1-7`-SNAPSHOT"

mvn versions:set -DgenerateBackupPoms=false \
                 -DnewVersion=$VERSION

# replace groupids in pom files
find . -name "pom.xml" -exec sed -i "s/<groupId>org.zeromq/<groupId>$GROUPID/" {} \;

mvn package

CreateNativeJar

# prevent find from colliding against itself
TMP_DIR=`mktemp -d`
find . -name "jzmq-*.jar" -exec cp {} $TMP_DIR \;
find -name "pom.xml" -exec cp --parents {} $TMP_DIR \;
cp -r $TMP_DIR/* $DEPLOY_TARGET_DIR

# write native_pom.xml as an example how to deploy the native .jar
cat <<EOT >> $DEPLOY_TARGET_DIR/native_pom.xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <groupId>$GROUPID</groupId>
  <artifactId>{ARTIFACTID}</artifactId>
  <packaging>jar</packaging>
  <version>$VERSION</version>
</project>
EOT

echo "\nYou can now deploy using the material in $DEPLOY_TARGET_DIR..."

## If you want to install on local maven repo without deploying use
## commands similar to this...

## JAR="jzmq/target/jzmq-$VERSION.jar"
## mvn install:install-file -Dfile="$JAR" \
##                          -DgroupId=$GROUPID \
##                          -Dversion=$VERSION \
##                          -Dpackaging=jar \
##                          -DartifactId=jzmq
