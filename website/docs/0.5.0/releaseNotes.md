---
title: Apache DistributedLog 0.5.0 Release Notes
layout: default
---

This is the second Apache release of DistributedLog and the first release as a sub-project of Apache BookKeeper!

The 0.5.0 release upgrades BookKeeper from Twitter `4.3.7-TWTTR-OSS` version to the official Apache `4.5.0` version.
It is a big milestone in Apache BookKeeper & DistributedLog community, converging the development efforts for
both communities and moving the DistributedLog development on the official Apache bookkeeper release.

Apache DistributedLog users are encouraged to upgrade to 0.5.0. The technical details of this release are summarized
below.

## Highlights

### BookKeeper Version Upgrade

The main change in 0.5.0 is BookKeeper version upgrade. We upgraded the BookKeeper version from Twitter's `4.3.7-TWTTR-OSS`
to the official [Apache BookKeeper `4.5.0`](http://bookkeeper.apache.org/docs/4.5.0/overview/releaseNotes/) version. This upgrade brings in BookKeeper security feature and performance improvement (such as netty 4 upgrade).
The upgrade should be straightforward, because there isn't any backward compatible issue.

### Change Twitter Future to Java Future

Prior to 0.5.0, the DistributedLog API heavily depends on Twitter Future library and other scala dependency. In 0.5.0, we change
the twitter future API to a separate module and replace the core API with Java8 CompletableFuture. It reduces the dependencies introduced
by Scala and also avoid maintaining multiple versions for different scala versions.

### Better buffer/memory management

After upgrading to BookKeeper 4.5.0, we update the buffer and memory management in DistributedLog to leverage netty's buffer management.
It reduces object allocation, memory copies and improves the latency because less jvm garbage is produced.


## Full list of changes

### JIRA


####Sub-task
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/DL-208'>DL-208</a>] -         Update release notes about the download location
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-209'>DL-209</a>] -         Update build script to build DistributedLog website under `bookkeeper.apache.org/distributedlog`
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-210'>DL-210</a>] -         Update DL mailing lists
</li>
</ul>
                            
####Bug
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/DL-19'>DL-19</a>] -         Can&#39;t compile thrift file
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-124'>DL-124</a>] -         Use Java8 Future rather than twitter Future
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-128'>DL-128</a>] -         All TODO items should have jira tickets.
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-195'>DL-195</a>] -         Apache Rat Check Failures on DISCLAIMER.bin.txt
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-206'>DL-206</a>] -         Delete log doesn&#39;t delete log segments
</li>
</ul>
                            
####Improvement
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/DL-204'>DL-204</a>] -         Bump libthrift to latest version for distributedlog-core
</li>
</ul>
                                                                            
####Task
<ul>
<li>[<a href='https://issues.apache.org/jira/browse/DL-2'>DL-2</a>] -         DistributedLog should work with the official apache bookkeeper
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-81'>DL-81</a>] -         Build the distributedlog release procedure
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-82'>DL-82</a>] -         Automatically build and update the website
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-199'>DL-199</a>] -         Be able to support filesystem-path like name
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-205'>DL-205</a>] -         Remove StatusCode dependency on DLException
</li>
<li>[<a href='https://issues.apache.org/jira/browse/DL-207'>DL-207</a>] -         DL Graduation Item
</li>
</ul>
                    

### Github

- [https://github.com/apache/distributedlog/milestone/1](https://github.com/apache/distributedlog/milestone/1?closed=1)
