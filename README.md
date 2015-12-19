# nats-messaging-for-nifi ![build status](https://travis-ci.org/mring33621/nats-messaging-for-nifi.svg?branch=master)
[NATS Messaging](http://nats.io/) processors for Apache [NiFi](http://nifi.apache.org/)

Version 0.5.1

ABOUT:
* Allows Apache NiFi to send/receive string based messages to/from NATS messaging topics.
* Based loosely on the Kafka processors that ship with NiFi.
* Tested on NiFi v0.4.0 and gnatsd Server v0.6.8
* Uses an embedded copy of https://github.com/mring33621/java_nats, which is a fork of https://github.com/tyagihas/java_nats, with some bug fixes.
* Java 7 or 8 compatible. Not sure about Java 6.

USAGE:
* Build the nifi-nats-bundle parent project with Maven. Make sure you clean all the target dirs first!
* Grab the .nar file from nifi-nats-nar/target and drop it into %NIFI_HOME%/lib.
* Restart NiFi.
* Now you can use the GetNats and PutNats processors.

LICENSE:
Apache 2.0 licensed.
