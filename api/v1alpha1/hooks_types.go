package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// Event defines the restore lifecycle point at which a hook job is triggered.
// +kubebuilder:validation:Enum=pre-restore;post-restore
type Event string

const (
	// PreRestore triggers the hook before the restore process begins.
	PreRestore Event = "pre-restore"

	// PostRestore triggers the hook after the restore process completes.
	PostRestore Event = "post-restore"
)

// Policy defines the restart policy for a hook job container.
type Policy string

const (
	// RestartPolicyNever does not restart the container on failure.
	RestartPolicyNever Policy = "Never"

	// RestartPolicyOnFailure restarts the container on failure.
	RestartPolicyOnFailure Policy = "OnFailure"
)

// ExtraSecretRef references a Secret whose keys are injected into the hook job
// container as environment variables. Each key is exposed as PREFIX_KEY,
// where PREFIX defaults to the secret name in upper snake case if not set.
type ExtraSecretRef struct {
	// Name is the name of the Secret in the same namespace.
	Name string `json:"name"`

	// Prefix is prepended to each env var name derived from the Secret's keys.
	// Defaults to the Secret name converted to UPPER_SNAKE_CASE with a trailing underscore.
	Prefix string `json:"prefix,omitempty"`
}

// NodeAccessPrivilege pins the hook job to nodes matching the selector
// and grants raw host-level access (hostPID + privileged SecurityContext).
// The operator fans out one Job per matching node under the hood.
type NodeAccessPrivilege struct {
	// NodeSelector selects the nodes this hook must run on.
	NodeSelector *metav1.LabelSelector `json:"nodeSelector"`
}

// HookPrivileges declares elevated access this hook needs beyond a standard
// Job container. Each capability is provisioned narrowly and torn down with
// the Job's TTL via OwnerReference on the CR. Declare only what the hook
// actually requires — these are real privilege-escalation surfaces.
type HookPrivileges struct {
	// PodExec lets the hook job kubectl-exec into pods in the same namespace.
	// The operator provisions a Role and RoleBinding scoped to pods/exec,
	// attached to the hook's own ServiceAccount. RBAC has no selector-scoped
	// permissions — the hook's command is responsible for only touching
	// the pods it means to.
	PodExec bool `json:"podExec,omitempty"`

	// NodeAccess pins the hook job to nodes matching the selector and grants
	// raw host-level access (hostPID + privileged SecurityContext).
	NodeAccess *NodeAccessPrivilege `json:"nodeAccess,omitempty"`
}

// +kubebuilder:validation:XValidation:rule="!self.mutable || !self.events.exists(e, e == 'pre-restore')",message="mutable cannot be used with pre-restore event"
type HookSettings struct {
	// Args are the arguments passed to the container command.
	Args []string `json:"args,omitempty"`

	// Command is the entrypoint executed in the container.
	Command []string `json:"command,omitempty"`

	// Events defines the restore lifecycle points at which this hook is triggered.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MaxItems=2
	Events []Event `json:"events"`

	// ExtraSecrets references Secrets whose keys are injected into the hook job
	// container as environment variables, each prefixed to indicate origin.
	ExtraSecrets []ExtraSecretRef `json:"extraSecrets,omitempty"`

	// Image is the container image used to run the hook job.
	Image string `json:"image"`

	// Mutable allows the hook job to be re-executed when its Command or Args change,
	// even if a previous run already succeeded. A checksum of Command and Args is
	// stored in status; on mismatch, the hook is treated as not succeeded and reruns.
	// Only honored for hooks triggered on the post-restore event; ignored otherwise.
	Mutable bool `json:"mutable,omitempty"`

	// Privileges declares elevated access this hook needs (pod exec, raw
	// node/disk access). Leave unset for a standard, unprivileged Job.
	Privileges *HookPrivileges `json:"privileges,omitempty"`

	// RestartPolicy defines the restart behavior of the hook job container.
	// Defaults to Never.
	// +kubebuilder:validation:Enum=Never;OnFailure
	RestartPolicy Policy `json:"restartPolicy,omitempty"`

	// Timeout defines the maximum time to wait for the hook job to complete.
	// If not set, defaults to 10 minutes. When exceeded, the hook is treated
	// as failed and will be retried on the next reconcile.
	Timeout *metav1.Duration `json:"timeout,omitempty"`
}
