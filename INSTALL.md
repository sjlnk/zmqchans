# Installation

Downloading from clojars is the easiest way to get started if you are using Linux with x86_64 architecture. Binary support for other os/architectures will be provided later. If there is a community need for it, they can be provided sooner. Just make an issue in github and it shall be investigated.

## Building libzmq

* jmzq is using libzmq under the hood. You need to build it first.

* Download a recent but stable version of libzmq. Refer to project.clj to get a version that is tested to work well with zmqchans: architecture specific jzmq dependencies show libzmq version at the end of the version string.

* Build libzmq and install it to some directory, from this point forward called `{LIBZMQ_DIR}`.

* Have environment varibles pointing to correct directories under `{LIBZMQ_DIR}`. On Linux in particular, prepend `{LIBZMQ_DIR}/include` to CPATH and `{LIBZMQ_DIR}/lib` to LD_LIBRARY_PATH.


## Building jzmq

* First make sure you have libzmq installed and available in your environment.

* Check out which repository the current version of zmqchans is using from the project.clj. You will find `[org.zmapi/jzmq "{VERSION}"]` or something similar in the dependencies section.

* Clone [jmzq](https://github.com/zeromq/jzmq).

* Checkout commit with SHA `{VERSION}`.

* Follow https://github.com/zeromq/jzmq/blob/master/README.md for instructions on how to install jzmq.

## Make native shared libraries available to zmqchans (leiningen)

### Method 1

* Go to jzmq-jni directory and do `make install` (configure first with ./configure and set --prefix to wherever you want to install this library.

* Make the output shared library available to you environment when you run java programs (LD_LIBRARY_PATH on linux).

### Method 2 (complex)

* Have a look at {ZMQCHANS_ROOT}/vendor/build.sh. This script is an example that builds jzmq and puts libzmq.so inside the platform specific native .jar. This script has been used for building the artifacts for deployment. With this method all the needed libraries are inside the jars installed to local maven repo -- no external library dependencies.

## Add profiles.clj or modify project.clj in zmqchans root

* Go to zmqchans root directory

* Remove old references to jzmq in project.clj and add the new artifacts that you just installed in there.

* You only need to add `org.zeromq/jzmq`, it is a meta artifact that contains the other artifacts.

* See the artifacts themselves for hints on the correct versioning: `ls {M2_REPO_DIR}/org/zeromq`. {M2_REPO_DIR} is `~/.m2/repository` on my Linux machine.

* You can alternatively add a profiles.clj to zmqchans root instead of modifying project.clj, refer to leiningen documentation on how to do this.

## Final notes

If you need to modify project.clj in order to get zmqchans working with your particular setup, don't include that file when making a pull request. The file in github repo is the one that works for most people but it cannot work for everyone. You need to learn how to personalize project.clj using profiles.clj if this becomes an issue.
