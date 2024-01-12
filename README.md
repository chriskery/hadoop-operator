# HadoopCluster Operator

[![Go Report](https://goreportcard.com/badge/github.com/chriskery/hadoop-cluster-operator)](https:/goreportcard.com/report/github.com/chriskery/hadoop-cluster-operator) 
[![Build Status](https://github.com/chriskery/hadoop-cluster-operator/actions/workflows/test-go.yml/badge.svg?branch=master)](https://github.com/chriskery/hadoop-cluster-operator/actions/workflows/test-go.yaml?branch=master)
[![Coverage Status](https://coveralls.io/repos/github/chriskery/hadoop-cluster-operator/badge.svg?branch=master)](https://coveralls.io/github/chriskery/hadoop-cluster-operator?branch=master)
[![Version](https://img.shields.io/github/release/chriskery/hadoop-cluster-operator.svg)](https:/github.com/chriskery/hadoop-cluster-operator/releases)

## Overview

[Hadoop](https://hadoop.apache.org/) is a framework that allows for the distributed processing of large data sets across clusters of computers using simple programming models. It is designed to scale up from single servers to thousands of machines, each offering local computation and storage. Rather than rely on hardware to deliver high-availability, the library itself is designed to detect and handle failures at the application layer, providing a highly-available service on top of a cluster of computers, each of which may be prone to failures

The hadoop-cluster Operator manages hadoop clusters deployed to Kubernetes.

## Install the Operator
### Cert Manager
hadoop cluster operator relies on [cert-manager](https://cert-manager.io/docs/installation/), you must have cert-manager installed and configured in your Kubernetes cluster. cert-manager is a Kubernetes add-on that automates the management of certificates and is a requirement for this Operator to function correctly.

Installing cert-manager:

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.13.3/cert-manager.yaml
```

📖 Read more about [installing cert-manager using kubectl apply and static manifests](https://cert-manager.io/docs/installation/kubectl/).

### Master Branch
```shell
kubectl apply -k github.com/chriskery/hadoop-cluster-operator/manifests/default
```

## Quick Start

Please refer to the [quick-start.md](docs/quick-start.md)  for more information.


## Features

- [x] [Create and destroy a hadoop-cluster](https://github.com/chriskery/charts/tree/master/charts/hadoop-cluster#deploying-hadoop-cluster)
- [x] [Hdfs DataNode HPA](https://github.com/chriskery/charts/tree/master/charts/hadoop-cluster#deploying-hadoop-cluster)
- [x] [Yarn NodeManager HPA](https://github.com/chriskery/charts/tree/master/charts/hadoop-cluster#deploying-hadoop-cluster)
- [x] [Submit Hadoop Job](https://github.com/chriskery/charts/tree/master/charts/hadoop-cluster#deploying-hadoop-cluster)

## Contributing and Community

We thrive to build a welcoming and open community for anyone who wants to use the operator or contribute to it. 

