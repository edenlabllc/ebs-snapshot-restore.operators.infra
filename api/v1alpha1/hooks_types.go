package v1alpha1

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

type HookSettings struct {
	// Args are the arguments passed to the container command.
	Args []string `json:"args,omitempty"`

	// Command is the entrypoint executed in the container.
	Command []string `json:"command,omitempty"`

	// Events defines the restore lifecycle points at which this hook is triggered.
	// +kubebuilder:validation:Required
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

	// RestartPolicy defines the restart behavior of the hook job container.
	// Defaults to Never.
	// +kubebuilder:validation:Enum=Never;OnFailure
	RestartPolicy Policy `json:"restartPolicy,omitempty"`
}
