/*
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
*/

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type RestorePlan struct {
	// Clusters defines the list of StatefulSet workloads to restore.
	// Each cluster entry describes a target StatefulSet and its associated PVC snapshots.
	Clusters []RestoreTarget `json:"clusters"`

	// Operators defines the list of operator deployments to scale down before restore
	// and scale up after restore is complete. Optional.
	Operators []RestoreTarget `json:"operators,omitempty"`

	// SnapshotRestoreTime specifies the exact snapshot timestamp to restore from,
	// in YYYYMMDDHHMM format (e.g. "202604071537").
	// If not set, the latest available snapshot will be used automatically.
	SnapshotRestoreTime string `json:"snapshotRestoreTime,omitempty"`
}

type RestoreTarget struct {
	// Name is the name of the target workload (StatefulSet or Deployment).
	Name string `json:"name"`

	// Type specifies the workload kind. Supported values: "statefulset", "deployment".
	Type string `json:"type"`

	// Replicas is the desired number of replicas to restore the workload to
	// after the restore process is complete.
	Replicas int32 `json:"replicas"`

	// Namespace is the Kubernetes namespace where the target workload resides.
	Namespace string `json:"namespace"`

	// SkipRestoringPVCs is a list of PVC names to skip during restore.
	// PVCs in this list will not be recreated from snapshots and will retain
	// their current state. Useful when some volumes do not require restoration.
	SkipRestoringPVCs []string `json:"skipRestoringPVCs,omitempty"`

	// ParallelPodManagement enables parallel pod management for the StatefulSet.
	// When set to true, all pods are started simultaneously instead of sequentially.
	// This is useful when restoring from snapshots to minimize oplog divergence between
	// replica set members, which can cause rollback issues on large datasets.
	// Defaults to false (OrderedReady behavior).
	ParallelPodManagement bool `json:"parallelPodManagement,omitempty"`

	// ClaimSelector is a label selector used to identify PVCs associated with this target.
	// Required for StatefulSet targets. Not needed for Deployments.
	// +optional
	ClaimSelector *metav1.LabelSelector `json:"claimSelector,omitempty"`
}

// EBSSnapshotRestoreSpec defines the desired state of EBSSnapshotRestore
type EBSSnapshotRestoreSpec struct {
	RestorePlans map[string]RestorePlan `json:"restorePlans,omitempty"`
}

// EBSSnapshotRestoreStatus defines the observed state of EBSSnapshotRestore.
type EBSSnapshotRestoreStatus struct {
	// High-level phase of the entire restore process
	Phase RestorePhase `json:"phase,omitempty"`

	// Currently active restore plan
	ActivePlan string `json:"activePlan,omitempty"`

	// Detailed status for each restore plan
	RestorePlans map[string]RestorePlanStatus `json:"restorePlans,omitempty"`

	// Timestamp of the last update
	// +optional
	LastUpdated *metav1.Time `json:"lastUpdated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="PHASE",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="ACTIVE-PLAN",type=string,JSONPath=`.status.activePlan`
// +kubebuilder:printcolumn:name="LAST-UPDATED",type=date,JSONPath=`.status.lastUpdated`

// EBSSnapshotRestore is the Schema for the ebssnapshotrestores API
type EBSSnapshotRestore struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of EBSSnapshotRestore
	// +required
	Spec EBSSnapshotRestoreSpec `json:"spec"`

	// status defines the observed state of EBSSnapshotRestore
	// +optional
	Status EBSSnapshotRestoreStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// EBSSnapshotRestoreList contains a list of EBSSnapshotRestore
type EBSSnapshotRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EBSSnapshotRestore `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EBSSnapshotRestore{}, &EBSSnapshotRestoreList{})
}
