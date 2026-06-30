package hooks

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/net/context"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
	"ebs-snapshot-restore.operators.infra/internal/status"
)

const (
	// hookJobTTLSeconds keeps a finished hook Job around briefly for inspection,
	// then lets the TTL controller clean it up — short enough that a re-run
	// of a mutable hook on the next reconcile won't hit AlreadyExists.
	hookJobTTLSeconds = 60

	// hookJobBackoffLimit caps retries per hook Job: 1 initial attempt + 2 retries.
	hookJobBackoffLimit = 0

	pollInterval = 5 * time.Second
	pollTimeout  = 10 * time.Minute
)

type ManageHooks struct {
	Client client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger
	Status *status.ManageStatus
}

type hookJobSettings struct {
	Name          string
	Namespace     string
	Args          []string
	Checksum      string
	Command       []string
	Event         ebsv1alpha1.Event
	ExtraSecrets  []ebsv1alpha1.ExtraSecretRef
	Image         string
	Mutable       bool
	OwnerMeta     metav1.OwnerReference
	PlanName      string
	RestartPolicy ebsv1alpha1.Policy
}

func New(c client.Client, s *runtime.Scheme, l logr.Logger, status *status.ManageStatus) *ManageHooks {
	return &ManageHooks{Client: c, Scheme: s, Logger: l.WithName("Hooks"), Status: status}
}

func hookChecksum(command, args []string) string {
	h := sha256.New()
	for _, c := range command {
		h.Write([]byte(c))
		h.Write([]byte{0})
	}

	for _, a := range args {
		h.Write([]byte(a))
		h.Write([]byte{0})
	}

	return fmt.Sprintf("%x", h.Sum(nil))[:8]
}

func hookJobName(planName, hookName string) string {
	const (
		maxLen     = 63
		sep        = "-rh-"
		hashSuffix = "-xxxxxxxx"
	)

	full := planName + sep + hookName
	if len(full) <= maxLen {
		return full
	}

	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(full)))[:8]
	budget := maxLen - len(hashSuffix)
	perPart := (budget - len(sep)) / 2

	p := planName
	if len(p) > perPart {
		p = strings.TrimRight(p[:perPart], "-")
	}

	h := hookName
	if len(h) > perPart {
		h = strings.TrimRight(h[:perPart], "-")
	}

	return fmt.Sprintf("%s%s%s-%s", p, sep, h, hash)
}

func (m *ManageHooks) SetupRestoreHooks(event ebsv1alpha1.Event, obj *ebsv1alpha1.EBSSnapshotRestore) ([]hookJobSettings, error) {
	var hJS []hookJobSettings

	ownerGVK, err := apiutil.GVKForObject(obj, m.Scheme)
	if err != nil {
		return nil, err
	}

	ownerRef := metav1.OwnerReference{
		APIVersion:         ownerGVK.GroupVersion().String(),
		Kind:               ownerGVK.Kind,
		Name:               obj.GetName(),
		UID:                obj.GetUID(),
		Controller:         ptr.To(true),
		BlockOwnerDeletion: ptr.To(true),
	}

	planNames := make([]string, 0, len(obj.Spec.RestorePlans))
	for k := range obj.Spec.RestorePlans {
		planNames = append(planNames, k)
	}

	sort.Strings(planNames)

	for _, planName := range planNames {
		plan := obj.Spec.RestorePlans[planName]
		hookNames := make([]string, 0, len(plan.Hooks))
		for hookName := range plan.Hooks {
			hookNames = append(hookNames, hookName)
		}

		sort.Strings(hookNames)

		for _, hookName := range hookNames {
			hookSettings := plan.Hooks[hookName]
			for _, e := range hookSettings.Events {
				if e == event {
					hJS = append(hJS, hookJobSettings{
						Name:          hookJobName(planName, hookName),
						Namespace:     obj.GetNamespace(),
						Args:          hookSettings.Args,
						Checksum:      hookChecksum(hookSettings.Command, hookSettings.Args),
						Command:       hookSettings.Command,
						Event:         event,
						ExtraSecrets:  hookSettings.ExtraSecrets,
						Image:         hookSettings.Image,
						Mutable:       hookSettings.Mutable,
						OwnerMeta:     ownerRef,
						PlanName:      planName,
						RestartPolicy: hookSettings.RestartPolicy,
					})
				}
			}
		}
	}

	return hJS, nil
}

func (m *ManageHooks) RunHooks(ctx context.Context, hooks []hookJobSettings, obj *ebsv1alpha1.EBSSnapshotRestore) error {
	byPlan := make(map[string][]hookJobSettings)
	for _, h := range hooks {
		byPlan[h.PlanName] = append(byPlan[h.PlanName], h)
	}

	for planName, planHooks := range byPlan {
		var pending []hookJobSettings

		for _, hook := range planHooks {
			skip, reason := hookSkipReason(obj, planName, hook.Name, hook.Event, hook.Mutable, hook.Checksum)
			if skip {
				m.Logger.Info("hook job skipped", "name", hook.Name, "event", hook.Event)
				continue
			}

			m.Logger.Info("hook job will run", "name", hook.Name, "event", hook.Event, "reason", reason)

			pending = append(pending, hook)
		}

		if err := m.runPlanHooks(ctx, obj, planName, pending); err != nil {
			if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
				return err
			}

			return err
		}
	}

	return nil
}

func hookSkipReason(obj *ebsv1alpha1.EBSSnapshotRestore, planName, hookName string,
	event ebsv1alpha1.Event, mutable bool, checksum string) (skip bool, reason string) {
	plan, ok := obj.Status.RestorePlans[planName]
	if !ok {
		// first reconcile for this plan — nothing has ever run, so this hook must run
		return false, "no prior status for plan"
	}

	// once the snapshot has been restored (Lock=true), pre-restore hooks
	// no longer apply — the restore point they prepare for has already passed
	if plan.Lock && event == ebsv1alpha1.PreRestore {
		return true, "snapshot already restored, pre-restore hooks no longer apply"
	}

	for _, h := range plan.Hooks {
		if h.Name != hookName || h.Event != event {
			// status entry belongs to a different hook or event — keep looking
			continue
		}

		if h.Succeeded == 0 {
			// this exact hook was attempted before but failed (or hasn't finished) — must run again
			return false, "previous run not successful"
		}

		if mutable && event == ebsv1alpha1.PostRestore && h.Checksum != checksum {
			// hook is marked mutable and its Command/Args changed since the last
			// successful run — treat as not-yet-done so it reruns with the new checksum
			return false, fmt.Sprintf("checksum changed %s -> %s", h.Checksum, checksum)
		}

		// same hook, same event, succeeded before, and (if mutable) checksum unchanged
		return true, "already succeeded"
	}

	// status exists for the plan, but not for this specific hook+event combination —
	// e.g. a hook that was just added to the spec — so it must run
	return false, "no prior status for this hook"
}

func (m *ManageHooks) runPlanHooks(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, planName string, hooks []hookJobSettings) error {
	for _, hook := range hooks {
		job, err := m.buildJob(hook)
		if err != nil {
			return fmt.Errorf("build hook job %s: %w", hook.Name, err)
		}

		m.Logger.Info("creating hook job", "name", job.Name, "namespace", job.Namespace)

		if err := m.Client.Create(ctx, job); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create hook job %s: %w", job.Name, err)
			}

			m.Logger.Info("hook job already exists, waiting", "name", job.Name)
		}

		m.Logger.Info("waiting for hook job", "name", job.Name)

		hookStatus := ebsv1alpha1.RestoreHookStatus{
			Name:          job.Name,
			Namespace:     job.Namespace,
			Checksum:      hook.Checksum,
			Event:         hook.Event,
			Image:         hook.Image,
			RestartPolicy: hook.RestartPolicy,
		}

		if err := m.waitForJob(ctx, job); err != nil {
			// save failed status before returning
			hookStatus.Failed = 1
			_ = m.Status.SetHooksRestoreFromPlan(ctx, obj, planName, hookStatus)
			return fmt.Errorf("hook job %s: %w", job.Name, err)
		}

		m.Logger.Info("hook job completed successfully", "name", job.Name)

		// update status incrementally after each hook
		hookStatus.Succeeded = 1
		if err := m.Status.SetHooksRestoreFromPlan(ctx, obj, planName, hookStatus); err != nil {
			return err
		}
	}

	return nil
}

func defaultEnvPrefix(secretName string) string {
	upper := strings.ToUpper(secretName)
	sanitized := strings.NewReplacer("-", "_", ".", "_").Replace(upper)
	return sanitized + "_"
}

func normalizePrefix(prefix string) string {
	if !strings.HasSuffix(prefix, "_") {
		return prefix + "_"
	} else {
		return prefix
	}
}

func (m *ManageHooks) buildJob(h hookJobSettings) (*batchv1.Job, error) {
	gvk, err := apiutil.GVKForObject(&batchv1.Job{}, m.Scheme)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{"app": h.Name, "hook-type": string(h.Event), "owner": h.OwnerMeta.Name}

	container := v1.Container{
		Name:    h.Name,
		Image:   h.Image,
		Command: h.Command,
		Args:    h.Args,
	}

	for _, secret := range h.ExtraSecrets {
		prefix := secret.Prefix
		if prefix == "" {
			prefix = defaultEnvPrefix(secret.Name)
		} else {
			prefix = normalizePrefix(prefix)
		}

		container.EnvFrom = append(container.EnvFrom, v1.EnvFromSource{
			Prefix: prefix,
			SecretRef: &v1.SecretEnvSource{
				LocalObjectReference: v1.LocalObjectReference{Name: secret.Name},
			},
		})
	}

	restartPolicy := h.RestartPolicy
	if restartPolicy == "" {
		restartPolicy = ebsv1alpha1.RestartPolicyNever
	}

	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{
			Kind:       gvk.Kind,
			APIVersion: gvk.GroupVersion().String(),
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      h.Name,
			Namespace: h.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         h.OwnerMeta.APIVersion,
					Kind:               h.OwnerMeta.Kind,
					Name:               h.OwnerMeta.Name,
					UID:                h.OwnerMeta.UID,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				},
			},
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: ptr.To(int32(hookJobTTLSeconds)),
			BackoffLimit:            ptr.To(int32(hookJobBackoffLimit)),
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: v1.PodSpec{
					Containers:    []v1.Container{container},
					RestartPolicy: v1.RestartPolicy(restartPolicy),
				},
			},
		},
	}, nil
}

func (m *ManageHooks) waitForJob(ctx context.Context, job *batchv1.Job) error {
	var seen bool
	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			current := &batchv1.Job{}

			err := m.Client.Get(ctx, types.NamespacedName{
				Name:      job.Name,
				Namespace: job.Namespace,
			}, current)
			if err != nil {
				if apierrors.IsNotFound(err) {
					if seen {
						return false, fmt.Errorf("job %s/%s was deleted externally", job.Namespace, job.Name)
					}

					m.Logger.Info("job not yet visible in cache, retrying", "namespace", job.Namespace, "name", job.Name)
					return false, nil
				}

				return false, err
			}

			seen = true

			if current.Status.Succeeded > 0 {
				return true, nil
			}

			if current.Status.Failed > 0 && current.Status.Active == 0 {
				return false, fmt.Errorf("job failed after %d attempt(s)", current.Status.Failed)
			}

			return false, nil
		},
	)
}
