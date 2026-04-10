# ebs-snapshot-restore

[![Release](https://img.shields.io/github/v/release/edenlabllc/ebs-snapshot-restore.operators.infra.svg?style=for-the-badge)](https://github.com/edenlabllc/ebs-snapshot-restore.operators.infra/releases/latest)
[![Software License](https://img.shields.io/github/license/edenlabllc/ebs-snapshot-restore.operators.infra.svg?style=for-the-badge)](LICENSE)
[![Powered By: Edenlab](https://img.shields.io/badge/powered%20by-edenlab-8A2BE2.svg?style=for-the-badge)](https://edenlab.io)

A Kubernetes operator for automated restoration of EBS volumes from snapshots.
Designed to restore stateful workloads (StatefulSets) and their associated PersistentVolumeClaims
from pre-existing VolumeSnapshots created by [SnapScheduler](https://backube.github.io/snapscheduler/).

## Description

`ebs-snapshot-restore` is a Kubernetes operator built with [controller-runtime](https://github.com/kubernetes-sigs/controller-runtime)
that automates the process of restoring EBS-backed PersistentVolumeClaims from VolumeSnapshots.

The operator manages the full restore lifecycle through the `EBSSnapshotRestore` custom resource:

1. **Validation** — finds and validates VolumeSnapshots for each target workload by matching
   PVC labels and snapshot timestamps. Supports explicit restore time (`snapshotRestoreTime`)
   or automatic selection of the latest available snapshot.

2. **Scale Down** — safely scales down target StatefulSets and associated operator Deployments
   to zero replicas before performing restore, ensuring data consistency.

3. **Restore** — deletes existing PVCs and recreates them from VolumeSnapshots,
   waiting for each PVC to be fully bound before proceeding.

4. **Scale Up** — scales workloads back to their original replica count
   and waits for all pods to become ready.

5. **Lock** — once a restore plan completes successfully, it is locked to prevent
   accidental re-execution. The lock can be explicitly released by setting `lock: false`
   in the restore plan spec.

### Supported workload types
- `statefulset` — primary restore target with PVC management
- `deployment` — operator/controller deployments that need to be paused during restore

### Supported storage
- AWS EBS volumes provisioned via `ebs.csi.aws.com`
- VolumeSnapshots managed by [SnapScheduler](https://backube.github.io/snapscheduler/)

## Getting Started

### Prerequisites
- go version v1.24.0+
- docker version 17.03+.
- kubectl version v1.11.3+.
- Access to a Kubernetes v1.11.3+ cluster.

### To Deploy on the cluster
**Build and push your image to the location specified by `IMG`:**

```sh
make docker-build docker-push IMG=<some-registry>/ebs-snapshot-restore:tag
```

**NOTE:** This image ought to be published in the personal registry you specified.
And it is required to have access to pull the image from the working environment.
Make sure you have the proper permission to the registry if the above commands don’t work.

**Install the CRDs into the cluster:**

```sh
make install
```

**Deploy the Manager to the cluster with the image specified by `IMG`:**

```sh
make deploy IMG=<some-registry>/ebs-snapshot-restore:tag
```

> **NOTE**: If you encounter RBAC errors, you may need to grant yourself cluster-admin
privileges or be logged in as admin.

**Create instances of your solution**
You can apply the samples (examples) from the config/sample:

```sh
kubectl apply -k config/samples/
```

>**NOTE**: Ensure that the samples has default values to test it out.

### To Uninstall
**Delete the instances (CRs) from the cluster:**

```sh
kubectl delete -k config/samples/
```

**Delete the APIs(CRDs) from the cluster:**

```sh
make uninstall
```

**UnDeploy the controller from the cluster:**

```sh
make undeploy
```

## Project Distribution

Following the options to release and provide this solution to the users.

### By providing a bundle with all YAML files

1. Build the installer for the image built and published in the registry:

```sh
make build-installer IMG=<some-registry>/ebs-snapshot-restore:tag
```

**NOTE:** The makefile target mentioned above generates an 'install.yaml'
file in the dist directory. This file contains all the resources built
with Kustomize, which are necessary to install this project without its
dependencies.

2. Using the installer

Users can just run 'kubectl apply -f <URL for YAML BUNDLE>' to install
the project, i.e.:

```sh
kubectl apply -f https://raw.githubusercontent.com/<org>/ebs-snapshot-restore/<tag or branch>/dist/install.yaml
```

### By providing a Helm Chart

1. Build the chart using the optional helm plugin

```sh
kubebuilder edit --plugins=helm/v1-alpha
```

2. See that a chart was generated under 'dist/chart', and users
can obtain this solution from there.

**NOTE:** If you change the project, you need to update the Helm Chart
using the same command above to sync the latest changes. Furthermore,
if you create webhooks, you need to use the above command with
the '--force' flag and manually ensure that any custom configuration
previously added to 'dist/chart/values.yaml' or 'dist/chart/manager/manager.yaml'
is manually re-applied afterwards.

## Contributing

**NOTE:** Run `make help` for more information on all potential `make` targets

More information can be found via the [Kubebuilder Documentation](https://book.kubebuilder.io/introduction.html)

## License

Copyright 2026 Edenlab.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
