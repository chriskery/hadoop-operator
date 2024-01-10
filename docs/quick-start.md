## Create a Hadoop Cluster Instance

To create a Hadoop Cluster instance, a HadoopCluster resource definition must be created and applied. HadoopCluster Kubernetes Operator creates the necessary resources, such as Services and StatefulSet, in the same namespace in which the HadoopCluster was defined.

First, create a YAML file to define a HadoopCluster resource named **hello-world.yaml**.

Then copy and paste the below snippet into the file and save it:

```yaml
apiVersion: kubecluster.org/v1alpha1
kind: HadoopCluster
metadata:
  name: hadoopcluster-sample
spec:
  yarn:
    serviceType: NodePort
```

Next, apply the **hello-world.yaml** by running:

```bash
kubectl apply -f hello-world.yaml
```

Now that we have created a Hadoop Cluster, run the following command to view it:

```bash
kubectl get hdcs
```

Output similar to：

```bash
kubectl get all -l app.kubernetes.io/name=definition






```
