package v1alpha1

import (
	appsv1 "k8s.io/api/apps/v1"
)

// RestorePhase represents the current phase of a restore operation or target.
type RestorePhase string

const (
	// PhaseCompleted indicates that the restore process has finished successfully.
	PhaseCompleted RestorePhase = "Completed"

	// PhaseFailed indicates that the restore process has encountered an error and stopped.
	PhaseFailed RestorePhase = "Failed"

	// PhaseIdle indicates that the restore process has not started yet.
	PhaseIdle RestorePhase = "Idle"

	// PhaseRestored indicates that PVCs have been successfully restored from snapshots.
	PhaseRestored RestorePhase = "Restored"

	// PhaseRestoring indicates that PVC restoration from snapshots is currently in progress.
	PhaseRestoring RestorePhase = "Restoring"

	// PhaseScalingDown indicates that the target workload is currently being scaled down to zero.
	PhaseScalingDown RestorePhase = "ScalingDown"

	// PhaseScaledDown indicates that the target workload has been successfully scaled down to zero.
	PhaseScaledDown RestorePhase = "ScaledDown"

	// PhaseRunningPreHooks indicates pre-restore hook jobs are currently executing,
	// before the snapshot restore itself begins.
	PhaseRunningPreHooks RestorePhase = "RunningPreHooks"

	// PhaseRunningPostHooks indicates post-restore hook jobs are currently executing,
	// after the snapshot restore has completed.
	PhaseRunningPostHooks RestorePhase = "RunningPostHooks"

	// PhaseScalingUp indicates that the target workload is currently being scaled back up to original replicas.
	PhaseScalingUp RestorePhase = "ScalingUp"

	// PhaseValidated indicates that snapshots have been found and validated successfully.
	PhaseValidated RestorePhase = "Validated"

	// PhaseValidating indicates that snapshot validation is currently in progress.
	PhaseValidating RestorePhase = "Validating"
)

// ConfigMapReference is a reference to a ConfigMap.
type ConfigMapReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// RestoreHookStatus reflects the execution result of a single hook job.
type RestoreHookStatus struct {
	// Name is the name of the hook Job resource.
	Name string `json:"name"`

	// Namespace is the namespace where the hook Job was created.
	Namespace string `json:"namespace"`

	// Checksum is the hash of Command and Args used in the last execution.
	// Used together with Mutable to detect changes and trigger a rerun.
	Checksum string `json:"checksum,omitempty"`

	// Event is the restore lifecycle point at which this hook was triggered.
	Event Event `json:"event,omitempty"`

	// Failed is the number of times this hook job failed.
	Failed int32 `json:"failed,omitempty"`

	// Image is the container image used to run the hook job.
	Image string `json:"image,omitempty"`

	// RestartPolicy is the restart policy applied to the hook job container.
	RestartPolicy Policy `json:"restartPolicy,omitempty"`

	// Succeeded is the number of times this hook job completed successfully.
	Succeeded int32 `json:"succeeded,omitempty"`

	// ConfigMapRef references the ConfigMap storing the original
	// Command and Args for this hook.
	ConfigMapRef *ConfigMapReference `json:"configMapRef,omitempty"`
}

// RestoreSnapshotRef holds a reference to a VolumeSnapshot associated with a specific PVC.
type RestoreSnapshotRef struct {
	// PVCName is the name of the PersistentVolumeClaim associated with this snapshot.
	PVCName string `json:"pvcName,omitempty"`

	// PVCStorageClass is the StorageClass used when restoring this PVC.
	// Nil if the original StorageClass was used.
	PVCStorageClass *string `json:"pvcStorageClass,omitempty"`

	// SnapshotUID is the unique identifier of the VolumeSnapshot resource.
	SnapshotUID string `json:"snapshotUID,omitempty"`

	// SnapshotName is the name of the VolumeSnapshot resource used for restore.
	SnapshotName string `json:"snapshotName,omitempty"`

	// SkipRestoringPVC indicates whether PVC restoration was skipped for this target.
	// When true, the PVC was not recreated from a snapshot and retains its current state.
	SkipRestoringPVC bool `json:"skipRestoringPVC"`
}

// RestoreTargetStatus reflects the observed state of a single restore target (StatefulSet or Deployment).
type RestoreTargetStatus struct {
	// Name is the name of the target workload.
	Name string `json:"name,omitempty"`

	// Namespace is the Kubernetes namespace of the target workload.
	Namespace string `json:"namespace,omitempty"`

	// Type is the kind of the target workload (e.g. "statefulset", "deployment").
	Type string `json:"type,omitempty"`

	// SnapshotsRef contains references to the VolumeSnapshots used for restoring this target's PVCs.
	SnapshotsRef []RestoreSnapshotRef `json:"snapshotsRef,omitempty"`

	// PodManagementPolicy is the pod management policy applied to the StatefulSet during restore.
	PodManagementPolicy appsv1.PodManagementPolicyType `json:"podManagementPolicy,omitempty"`

	// OriginalReplicas is the number of replicas the workload had before scaling down.
	// Used to restore the workload to its original desired state after restore.
	OriginalReplicas int32 `json:"originalReplicas"`

	// CurrentReplicas is the number of replicas observed during the restore process.
	CurrentReplicas int32 `json:"currentReplicas"`

	// Phase is the current phase of this specific restore target.
	Phase RestorePhase `json:"phase,omitempty"`

	// Error contains the error message if this target encountered a failure during restore.
	Error string `json:"error,omitempty"`
}

type RestorePlanStatus struct {
	// Snapshot timestamp used for restore (logical restore point).
	SnapshotRestoreTime string `json:"snapshotRestoreTime,omitempty"`

	// Indicates whether post-restore unlock step has been completed.
	Lock bool `json:"lock,omitempty"`

	// Tracks the number of scale-up attempts performed during restore.
	CountScaleUp int32 `json:"countScaleUp,omitempty"`

	// Progress and state of cluster targets (e.g. StatefulSets).
	Clusters []RestoreTargetStatus `json:"clusters,omitempty"`

	// Progress and state of operator components (e.g. Deployments).
	Operators []RestoreTargetStatus `json:"operators,omitempty"`

	// Hooks contains the execution status of all hook jobs triggered during this plan's restore.
	Hooks []RestoreHookStatus `json:"hooks,omitempty"`
}
