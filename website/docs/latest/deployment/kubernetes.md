---
title: Docker
top-nav-group: deployment
top-nav-pos: 3
top-nav-title: Kubernetes
layout: default
---

Apache DistributedLog can be easily deployed in [Kubernetes](https://kubernetes.io/) clusters. The managed clusters on [Google Container Engine](https://cloud.google.com/compute/) is the most convenient way.

The deployment method shown in this guide relies on [YAML](http://yaml.org/) definitions for Kubernetes [resources](https://kubernetes.io/docs/resources-reference/v1.6/). The [`kubernetes`](https://github.com/apache/distributedlog/tree/master/deploy/kubernetes) subdirectory holds resource definitions for:

* A three-node ZooKeeper cluster
* A BookKeeper cluster with a bookie runs on each node.
* A three-node Proxy cluster.

If you already have setup a BookKeeper cluster following the instructions of [Deploying Apache BookKeeper on Kubernetes](http://bookkeeper.apache.org/docs/latest/deployment/kubernetes/) in Apache BookKeeper website,
you can skip deploying bookkeeper and start from [Create a DistributedLog Namespace](#create_namespace).

## Setup on Google Container Engine

To get started, get source code of [`kubernetes`](https://github.com/apache/distributedlog/tree/master/deploy/kubernetes) yaml definitions from github by git clone.

If you'd like to change the number of bookies, ZooKeeper nodes, or proxy nodes in your deployment, modify the `replicas` parameter in the `spec` section of the appropriate [`Deployment`](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/) or [`StatefulSet`](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) resource.

[Google Container Engine](https://cloud.google.com/container-engine) (GKE) automates the creation and management of Kubernetes clusters in [Google Compute Engine](https://cloud.google.com/compute/) (GCE).

### Prerequisites

To get started, you'll need:

* A Google Cloud Platform account, which you can sign up for at [cloud.google.com](https://cloud.google.com)
* An existing Cloud Platform project
* The [Google Cloud SDK](https://cloud.google.com/sdk/downloads) (in particular the [`gcloud`](https://cloud.google.com/sdk/gcloud/) and [`kubectl`]() tools).

### Create a new Kubernetes cluster

You can create a new GKE cluster using the [`container clusters create`](https://cloud.google.com/sdk/gcloud/reference/container/clusters/create) command for `gcloud`. This command enables you to specify the number of nodes in the cluster, the machine types of those nodes, and more.

As an example, we'll create a new GKE cluster for Kubernetes version [1.7.5](https://github.com/kubernetes/kubernetes/blob/master/CHANGELOG.md#v175) in the [us-central1-a](https://cloud.google.com/compute/docs/regions-zones/regions-zones#available) zone. The cluster will be named `bookkeeper-gke-cluster` and will consist of three VMs, each using two locally attached SSDs and running on [n1-standard-8](https://cloud.google.com/compute/docs/machine-types) machines. These SSDs will be used by Bookie instances, one for the BookKeeper journal and the other for storing the actual data.

```bash
$ gcloud config set compute/zone us-central1-a
$ gcloud config set project your-project-name
$ gcloud container clusters create bookkeeper-gke-cluster \
  --machine-type=n1-standard-8 \
  --num-nodes=3 \
  --local-ssd-count=2 \
  --cluster-version=1.7.5
```

By default, bookies will run on all the machines that have locally attached SSD disks. In this example, all of those machines will have two SSDs, but you can add different types of machines to the cluster later. You can control which machines host bookie servers using [labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/labels).

### Dashboard

You can observe your cluster in the [Kubernetes Dashboard](https://kubernetes.io/docs/tasks/access-application-cluster/web-ui-dashboard/) by downloading the credentials for your Kubernetes cluster and opening up a proxy to the cluster:

```bash
$ gcloud container clusters get-credentials bookkeeper-gke-cluster \
  --zone=us-central1-a \
  --project=your-project-name
$ kubectl proxy
```

By default, the proxy will be opened on port 8001. Now you can navigate to [localhost:8001/ui](http://localhost:8001/ui) in your browser to access the dashboard. At first your GKE cluster will be empty, but that will change as you begin deploying.

When you create a cluster, your `kubectl` config in `~/.kube/config` (on MacOS and Linux) will be updated for you, so you probably won't need to change your configuration. Nonetheless, you can ensure that `kubectl` can interact with your cluster by listing the nodes in the cluster:

```bash
$ kubectl get nodes
```

If `kubectl` is working with your cluster, you can proceed to deploy ZooKeeper and Bookies.

### ZooKeeper

You *must* deploy ZooKeeper as the first component, as it is a dependency for the others.

```bash
$ kubectl apply -f zookeeper.yaml
```

Wait until all three ZooKeeper server pods are up and have the status `Running`. You can check on the status of the ZooKeeper pods at any time:

```bash
$ kubectl get pods -l component=zookeeper
NAME      READY     STATUS             RESTARTS   AGE
zk-0      1/1       Running            0          18m
zk-1      1/1       Running            0          17m
zk-2      0/1       Running            6          15m
```

This step may take several minutes, as Kubernetes needs to download the Docker image on the VMs.


If you want to connect to one of the remote zookeeper server, you can use[zk-shell](https://github.com/rgs1/zk_shell), you need to forward a local port to the
remote zookeeper server:

```bash
$ kubectl port-forward zk-0 2181:2181
$ zk-shell localhost 2181
```

### Deploy Bookies

Once ZooKeeper cluster is Running, you can then deploy the bookies.

```bash
$ kubectl apply -f bookkeeper.yaml
```

You can check on the status of the Bookie pods for these components either in the Kubernetes Dashboard or using `kubectl`:

```bash
$ kubectl get pods
```

While all BookKeeper pods is Running, by zk-shell you could find all available bookies under /ledgers/

You can also verify the deployment by ssh to a bookie pod.

```bash
$ kubectl exec -it <pod_name> -- bash
```

On the bookie pod, you can run simpletest to verify the installation. The simpletest will create a ledger and append a few entries into the ledger.

```bash
$ BOOKIE_CONF=/opt/bookkeeper/conf/bk_server.conf /opt/distributedlog/bin/dlog bkshell simpletest
```

### Create DistributedLog Namespace

At this moment, you have a bookkeeper cluster up running on kubernetes. Now, You can create a distributedlog namespace and start playing with it.
If you setup the bookkeeper cluster following the above instructions, it uses `apachedistributedlog/distributedlog:0.5.0` image for running bookies.
You can skip creating distributedlog namespace here and move to next section. Because it already created a default
namespace `distributedlog://zookeeper/distributedlog` for you when starting the bookies.

You can create a distributedlog namespace using the `dlog` tool.

```bash
kubectl run dlog --rm=true --attach --image=apachedistributedlog/distributedlog:0.5.0 --restart=OnFailure -- /opt/distributedlog/bin/dlog admin bind -l /bookkeeper/ledgers -s zookeeper -c distributedlog://zookeeper/distributedlog
```

After you have a distributedlog namespace, you can play around the namespace by using `dlog` tool to create, delete, list and show the streams.

#### Create Streams

Create 10 streams prefixed with `mystream-`.

```bash
kubectl run dlog --rm=true --attach --image=apachedistributedlog/distributedlog:0.5.0 --restart=OnFailure -- /opt/distributedlog/bin/dlog tool create -u distributedlog://zookeeper/distributedlog -r mystream- -e 0-9 -f
```

#### List Streams

List the streams under the namespace.

```bash
kubectl run dlog --rm=true --attach --image=apachedistributedlog/distributedlog:0.5.0 --restart=OnFailure -- /opt/distributedlog/bin/dlog tool list -u distributedlog://zookeeper/distributedlog
```

An example of the output of this command is:

```
Streams under distributedlog://zookeeper/distributedlog :
--------------------------------
mystream-0
mystream-9
mystream-6
mystream-5
mystream-8
mystream-7
mystream-2
mystream-1
mystream-4
mystream-3
--------------------------------
```

### Un-Deploy

Delete BookKeeper
```bash
$ kubectl delete -f bookkeeper.yaml    
```

Delete ZooKeeper
```bash
$ kubectl delete -f zookeeper.yaml    
```

Delete cluster
```bash
$ gcloud container clusters delete bookkeeper-gke-cluster    
```
