---
layout: post
title:  'The first release of Apache DistributedLog'
date:   2017-04-25 10:00:00
categories: releases
authors:
- sijie
---

I’m happy to announce that Apache DistributedLog has officially released its first release under the apache umbrella - 0.4.0-incubating.
This is an exciting milestone for the project, which joined the Apache Software Foundation and the Apache Incubator last year.

This release publishes the first set of Apache DistributedLog binaries and source code, making them generally available to the community.
This initial release includes:

- distributedlog-core: A core library that provides the log stream storage primitives for accessing the stream store of Apache DistributedLog. 
- distributedlog-client & distributedlog-service: A proxy service (and its clients) for serving large number of fan-in writes and fan-out reads.
- distributedlog-benchmark: A set of programs for smoketesting the setup and benchmarking the performance of Apache DistributedLog.
- distributedlog-tutorials: A set of tutorials on how to use Apache DistributedLog in different use cases, e.g pub/sub messaging, replicated state machine and analytics.

The release is available in the [Maven Central Repository](https://search.maven.org/#search%7Cga%7C1%7Corg.apache.distributedlog),
or can be downloaded from the project’s [website](https://distributedlog.incubator.apache.org/docs/latest/start/download). 

As a community, we take every release seriously and try to ensure production readiness.
The first release is not just about following the Apache release process, i.e. creating a release and getting signoff from the Apache Software
Foundation--there is also tons of technical progress. The highlights of the first release are:

- Refactor the codebase to abstract the core metadata and data operations of the stream store into interfaces.
  So we can easily integrate DistributedLog with other metadata stores like etcd, and integrate DistributedLog with other cold data stores like HDFS, S3.
- A new read ahead implementation for DistributedLog reader for better handling slow storage nodes.
- Provide the stream placement policy on the proxy service. So we can develop and experiment with different placement policies and integrate
  with different auto-scaling mechanisms.

I’d like to encourage everyone to try out this release. 

As always, the DistributedLog community welcomes feedback. Simplifying building real-time applications is always the primary goal of Apache DistributedLog.
Usability, security, and integration will be our focus for the next several months.  Also, as we grow the community, a rapid cadence of future releases
is anticipated, perhaps one every 2~3 months.

If you have any comments or discover any issues, I’d like to invite you to reach out to us
via [user’s mailing list](https://distributedlog.incubator.apache.org/community/#mailing-lists), [slack channel](https://getdl-slack.herokuapp.com/) or
the [Apache Jira issues](https://issues.apache.org/jira/browse/DL).


