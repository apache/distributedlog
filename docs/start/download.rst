---
title: Download Releases
top-nav-group: setup
top-nav-pos: 2
top-nav-title: Download Releases
layout: default
---

.. contents:: This page covers how to download DistributedLog releases.

Releases
========

`0.4.0-incubating` is the latest release.

You can verify your download by checking its md5 and sha1.

0.4.0-incubating
~~~~~~~~~~~~~~~~

This is the first Apache release. Download here_.

- `Release Notes`_
- `Announce Blog Post`_

.. _here: https://dist.apache.org/repos/dist/release/bookkeeper/distributedlog/0.4.0-incubating
.. _Release Notes: https://issues.apache.org/jira/secure/ReleaseNote.jspa?projectId=12320620&version=12337980
.. _Announce Blog Post: /releases/2017/04/23/the-first-release.html


The releases before Apache Incubating are also listed as below:

0.3.51-RC1
~~~~~~~~~~

This is the second release candidate for 0.3.51.

- Source download: 0.3.51-RC1.zip_
- Binary downloads: 
    - Service: distributedlog-service-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip_
    - Benchmark: distributedlog-benchmark-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip_
    - Tutorials: distributedlog-tutorials-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip_
    - All: distributedlog-all-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip_

.. _0.3.51-RC1.zip: https://github.com/twitter/distributedlog/archive/0.3.51-RC1.zip
.. _distributedlog-all-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC1/distributedlog-all-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip
.. _distributedlog-service-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC1/distributedlog-service-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip
.. _distributedlog-benchmark-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC1/distributedlog-benchmark-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip
.. _distributedlog-tutorials-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC1/distributedlog-tutorials-3ff9e33fa577f50eebb8ee971ddb265c971c3717.zip

0.3.51-RC0
~~~~~~~~~~

This is the first release candidate for 0.3.51_.

- Source download: 0.3.51-RC0.zip_
- Binary downloads: 
    - Service: distributedlog-service-63d214d3a739cb58a71a8b51127f165d15f00584.zip_
    - Benchmark: distributedlog-benchmark-63d214d3a739cb58a71a8b51127f165d15f00584.zip_
    - Tutorials: distributedlog-tutorials-63d214d3a739cb58a71a8b51127f165d15f00584.zip_
    - All: distributedlog-all-63d214d3a739cb58a71a8b51127f165d15f00584.zip_

.. _0.3.51: https://github.com/twitter/distributedlog/releases/tag/0.3.51-RC0
.. _0.3.51-RC0.zip: https://github.com/twitter/distributedlog/archive/0.3.51-RC0.zip
.. _distributedlog-all-63d214d3a739cb58a71a8b51127f165d15f00584.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC0/distributedlog-all-63d214d3a739cb58a71a8b51127f165d15f00584.zip
.. _distributedlog-service-63d214d3a739cb58a71a8b51127f165d15f00584.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC0/distributedlog-service-63d214d3a739cb58a71a8b51127f165d15f00584.zip
.. _distributedlog-benchmark-63d214d3a739cb58a71a8b51127f165d15f00584.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC0/distributedlog-benchmark-63d214d3a739cb58a71a8b51127f165d15f00584.zip
.. _distributedlog-tutorials-63d214d3a739cb58a71a8b51127f165d15f00584.zip: https://github.com/twitter/distributedlog/releases/download/0.3.51-RC0/distributedlog-tutorials-63d214d3a739cb58a71a8b51127f165d15f00584.zip

Maven Dependencies
==================

You can add the following dependencies to your `pom.xml` to include Apache DistributedLog in your project.

.. code-block:: xml

  <!-- use core library to access DL storage -->
  <dependency>
    <groupId>org.apache.distributedlog</groupId>
    <artifactId>distributedlog-core_2.11</artifactId>
    <version>{{ site.DL_VERSION_STABLE }}</version>
  </dependency>
  <!-- use thin proxy client to access DL via write proxy -->
  <dependency>
    <groupId>org.apache.distributedlog</groupId>
    <artifactId>distributedlog-proxy-client_2.11</artifactId>
    <version>{{ site.DL_VERSION_STABLE }}</version>
  </dependency>


Version: 0.4.0 (the package is repackaged from com.twitter to org.apache.distributedlog)

.. code-block:: xml

  <!-- use core library to access DL storage -->
  <dependency>
    <groupId>org.apache.distributedlog</groupId>
    <artifactId>distributedlog-core_2.11</artifactId>
    <version>{{ site.DL_VERSION_STABLE }}</version>
  </dependency>
  <!-- use thin proxy client to access DL via write proxy -->
  <dependency>
    <groupId>org.apache.distributedlog</groupId>
    <artifactId>distributedlog-client_2.11</artifactId>
    <version>{{ site.DL_VERSION_STABLE }}</version>
  </dependency>

Version: 0.3.x

.. code-block:: xml

  <!-- use core library to access DL storage -->
  <dependency>
    <groupId>com.twitter</groupId>
    <artifactId>distributedlog-core_2.11</artifactId>
    <version>{{ site.DL_VERSION_STABLE }}</version>
  </dependency>
  <!-- use thin proxy client to access DL via write proxy -->
  <dependency>
    <groupId>com.twitter</groupId>
    <artifactId>distributedlog-client_2.11</artifactId>
    <version>{{ site.DL_VERSION_STABLE }}</version>
  </dependency>
