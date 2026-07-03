package hooks

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
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
	hookJobTTLSeconds = 30

	// hookJobBackoffLimit allows no retries — one attempt, immediate failure on error.
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
	Name             string
	Namespace        string
	Args             []string
	Checksum         string
	Command          []string
	Event            ebsv1alpha1.Event
	ExtraSecrets     []ebsv1alpha1.ExtraSecretRef
	Image            string
	Mutable          bool
	OwnerMeta        metav1.OwnerReference
	PlanName         string
	Privileges       *ebsv1alpha1.HookPrivileges
	ResolvedNodeName string
	RestartPolicy    ebsv1alpha1.Policy
	Timeout          *metav1.Duration
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

// perNodeJobName derives a unique Job name for a per-node fan-out of a hook.
// Uses a short hash of the node name since node FQDNs are often too long.
func perNodeJobName(baseJobName, nodeName string) string {
	const maxLen = 63
	hash := fmt.Sprintf("%x", sha256.Sum256([]byte(nodeName)))[:6]
	full := baseJobName + "-" + hash
	if len(full) <= maxLen {
		return full
	}

	maxBase := maxLen - 7 // "-" + 6 hash chars

	return strings.TrimRight(baseJobName[:maxBase], "-") + "-" + hash
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
						Privileges:    hookSettings.Privileges,
						RestartPolicy: hookSettings.RestartPolicy,
						Timeout:       hookSettings.Timeout,
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
			if hook.Privileges != nil && hook.Privileges.NodeAccess != nil {
				pending = append(pending, hook)
				continue
			}

			resolved, err := m.resolveHookFromConfigMap(ctx, hook)
			if err != nil {
				return err
			}

			m.logMutationStatus(obj, planName, resolved.Name, resolved.Event, resolved.Mutable, resolved.Checksum)

			skip, reason := hookSkipReason(obj, planName, resolved.Name, resolved.Event, resolved.Mutable, resolved.Checksum)
			if skip {
				m.Logger.Info("hook job skipped", "name", resolved.Name, "event", resolved.Event, "reason", reason)
				continue
			}

			m.Logger.Info("hook job will run", "name", resolved.Name, "event", resolved.Event, "reason", reason)

			pending = append(pending, resolved)
		}

		if err := m.runPlanHooks(ctx, obj, planName, pending); err != nil {
			if phaseErr := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); phaseErr != nil {
				return phaseErr
			}

			return err
		}
	}

	return nil
}

func (m *ManageHooks) logMutationStatus(obj *ebsv1alpha1.EBSSnapshotRestore, planName, hookName string, event ebsv1alpha1.Event, mutable bool, currentChecksum string) {
	if !mutable {
		return
	}

	plan, ok := obj.Status.RestorePlans[planName]
	if !ok {
		return
	}

	for _, h := range plan.Hooks {
		if h.Name == hookName && h.Event == event {
			m.Logger.Info("hook mutation status",
				"name", hookName,
				"event", event,
				"status_checksum", h.Checksum,
				"current_checksum", currentChecksum,
				"checksums_match", h.Checksum == currentChecksum,
				"succeeded", h.Succeeded,
			)
			return
		}
	}
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

// runHook executes a single hook job and writes its status immediately.
func (m *ManageHooks) runHook(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, planName string, hook hookJobSettings) error {
	hookStatus, err := m.executeHookJob(ctx, hook)
	if writeErr := m.Status.SetHooksRestoreFromPlan(ctx, obj, planName, hookStatus); writeErr != nil {
		return writeErr
	}

	return err
}

func (m *ManageHooks) runPlanHooks(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, planName string, hooks []hookJobSettings) error {
	for _, hook := range hooks {
		if hook.Privileges != nil && hook.Privileges.NodeAccess != nil {
			if phaseErr := m.runNodeAccessHook(ctx, obj, planName, hook); phaseErr != nil {
				return phaseErr
			}

			continue
		}

		if err := m.runHook(ctx, obj, planName, hook); err != nil {
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
	}

	return prefix
}

func (m *ManageHooks) buildJob(ctx context.Context, h hookJobSettings) (*batchv1.Job, error) {
	gvk, err := apiutil.GVKForObject(&batchv1.Job{}, m.Scheme)
	if err != nil {
		return nil, err
	}

	labels := map[string]string{
		"app":           h.Name,
		"hook-type":     string(h.Event),
		"owner":         h.OwnerMeta.Name,
		"hook-checksum": h.Checksum,
	}

	container := v1.Container{
		Name:    h.Name,
		Image:   h.Image,
		Command: h.Command,
		Args:    h.Args,
	}

	container.Env = append(container.Env,
		v1.EnvVar{
			Name: "NODE_NAME",
			ValueFrom: &v1.EnvVarSource{
				FieldRef: &v1.ObjectFieldSelector{
					FieldPath: "spec.nodeName",
				},
			},
		},
		v1.EnvVar{Name: "RESTORE_PLAN_NAME", Value: h.PlanName},
		v1.EnvVar{Name: "RESTORE_EVENT", Value: string(h.Event)},
		v1.EnvVar{Name: "RESTORE_CR_NAME", Value: h.OwnerMeta.Name},
	)

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

	pod := v1.PodSpec{RestartPolicy: v1.RestartPolicy(restartPolicy)}
	if err := m.applyPrivileges(ctx, h, &pod, &container); err != nil {
		return nil, err
	}

	pod.Containers = []v1.Container{container}

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
				Spec: pod,
			},
		},
	}, nil
}

func (m *ManageHooks) waitForJob(ctx context.Context, job *batchv1.Job, timeout *metav1.Duration) error {
	var (
		seen                  bool
		pollTimeoutWaitForJob time.Duration = pollTimeout
	)

	if timeout != nil {
		pollTimeoutWaitForJob = timeout.Duration
	}

	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeoutWaitForJob, true,
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

			m.Logger.Info("polling hook job status",
				"name", job.Name,
				"active", current.Status.Active,
				"succeeded", current.Status.Succeeded,
				"failed", current.Status.Failed,
			)

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

// ensureHookServiceAccount always provisions a dedicated ServiceAccount for
// the hook job. Every hook gets its own identity instead of sharing the
// namespace's default ServiceAccount — this makes audit logs attributable
// to a specific hook by name.
func (m *ManageHooks) ensureHookServiceAccount(ctx context.Context, h hookJobSettings) error {
	sa := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            h.Name,
			Namespace:       h.Namespace,
			OwnerReferences: []metav1.OwnerReference{h.OwnerMeta},
		},
	}
	if err := m.Client.Create(ctx, sa); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create service account %s: %w", h.Name, err)
	}

	return nil
}

// ensurePodExecRBAC provisions a Role and RoleBinding that grant pods/exec
// to the hook's own ServiceAccount. RBAC has no selector-scoped permissions —
// this covers the whole namespace; the hook's command is responsible for only
// touching the pods it means to.
func (m *ManageHooks) ensurePodExecRBAC(ctx context.Context, h hookJobSettings) error {
	role := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:            h.Name,
			Namespace:       h.Namespace,
			OwnerReferences: []metav1.OwnerReference{h.OwnerMeta},
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods/exec"},
				Verbs:     []string{"create"},
			},
		},
	}

	if err := m.Client.Create(ctx, role); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create role %s: %w", h.Name, err)
	}

	rb := &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            h.Name,
			Namespace:       h.Namespace,
			OwnerReferences: []metav1.OwnerReference{h.OwnerMeta},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      h.Name,
				Namespace: h.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     h.Name,
		},
	}

	if err := m.Client.Create(ctx, rb); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create role binding %s: %w", h.Name, err)
	}

	return nil
}

// applyPrivileges provisions any supporting objects and mutates pod/container
// specs based on declared HookPrivileges.
func (m *ManageHooks) applyPrivileges(ctx context.Context, h hookJobSettings, pod *v1.PodSpec, container *v1.Container) error {
	// every hook gets its own ServiceAccount regardless of Privileges
	if err := m.ensureHookServiceAccount(ctx, h); err != nil {
		return fmt.Errorf("provision service account for hook %s: %w", h.Name, err)
	}

	pod.ServiceAccountName = h.Name

	if h.Privileges == nil {
		return nil
	}

	if h.Privileges.PodExec {
		if err := m.ensurePodExecRBAC(ctx, h); err != nil {
			return fmt.Errorf("provision pod-exec rbac for hook %s: %w", h.Name, err)
		}
	}

	if h.Privileges.NodeAccess != nil {
		// ResolvedNodeName is set during fan-out in runNodeAccessHook
		pod.NodeName = h.ResolvedNodeName
		pod.HostPID = true
		pod.Tolerations = append(pod.Tolerations, v1.Toleration{Operator: v1.TolerationOpExists})
		container.SecurityContext = &v1.SecurityContext{Privileged: ptr.To(true)}
	}

	return nil
}

// resolveNodeNames returns names of all nodes matching the selector.
func (m *ManageHooks) resolveNodeNames(ctx context.Context, selector *metav1.LabelSelector) ([]string, error) {
	labelSelector, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return nil, fmt.Errorf("invalid nodeSelector: %w", err)
	}

	var nodeList v1.NodeList
	if err := m.Client.List(ctx, &nodeList, client.MatchingLabelsSelector{Selector: labelSelector}); err != nil {
		return nil, fmt.Errorf("list nodes: %w", err)
	}

	names := make([]string, 0, len(nodeList.Items))
	for _, node := range nodeList.Items {
		names = append(names, node.Name)
	}

	return names, nil
}

// executeHookJob creates and waits for a hook job, returning its final status.
// Does NOT write status — callers control when and how status is persisted,
// which is important for parallel execution where concurrent patches must be avoided.
func (m *ManageHooks) executeHookJob(ctx context.Context, hook hookJobSettings) (ebsv1alpha1.RestoreHookStatus, error) {
	hookStatus := ebsv1alpha1.RestoreHookStatus{
		Name:          hook.Name,
		Namespace:     hook.Namespace,
		Event:         hook.Event,
		Image:         hook.Image,
		RestartPolicy: hook.RestartPolicy,
		ConfigMapRef: &ebsv1alpha1.ConfigMapReference{
			Name:      hook.Name,
			Namespace: hook.Namespace,
		},
	}

	// ensure ConfigMap exists with original Command/Args
	if err := m.ensureHookConfigMap(ctx, hook); err != nil {
		return hookStatus, fmt.Errorf("ensure hook configmap %s: %w", hook.Name, err)
	}

	// always read Command/Args from ConfigMap — not from spec
	command, args, checksum, err := m.readHookConfigMap(ctx, hook)
	if err != nil {
		return hookStatus, err
	}

	hook.Command = command
	hook.Args = args
	hook.Checksum = checksum
	hookStatus.Checksum = checksum

	job, err := m.buildJob(ctx, hook)
	if err != nil {
		return hookStatus, fmt.Errorf("build hook job %s: %w", hook.Name, err)
	}

	m.Logger.Info("creating hook job", "name", job.Name, "namespace", job.Namespace)

	if err := m.Client.Create(ctx, job); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return hookStatus, fmt.Errorf("create hook job %s: %w", job.Name, err)
		}

		existing := &batchv1.Job{}
		if getErr := m.Client.Get(ctx, types.NamespacedName{
			Name:      job.Name,
			Namespace: job.Namespace,
		}, existing); getErr == nil {
			if existing.Labels["hook-checksum"] != hook.Checksum {
				m.Logger.Info("stale hook job detected, waiting for TTL cleanup",
					"name", job.Name,
					"existing_checksum", existing.Labels["hook-checksum"],
					"current_checksum", hook.Checksum,
				)

				return hookStatus, fmt.Errorf("hook job %s is stale (checksum mismatch), will retry after TTL", job.Name)
			}
		}

		m.Logger.Info("hook job already exists, waiting", "name", job.Name)
	}

	m.Logger.Info("waiting for hook job", "name", job.Name)

	if err := m.waitForJob(ctx, job, hook.Timeout); err != nil {
		hookStatus.Failed = 1
		return hookStatus, fmt.Errorf("hook job %s: %w", job.Name, err)
	}

	m.Logger.Info("hook job completed successfully", "name", job.Name)
	hookStatus.Succeeded = 1
	return hookStatus, nil
}

func (m *ManageHooks) runNodeAccessHook(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, planName string, hook hookJobSettings) error {
	nodeNames, err := m.resolveNodeNames(ctx, hook.Privileges.NodeAccess.NodeSelector)
	if err != nil {
		return fmt.Errorf("resolve nodes for hook %s: %w", hook.Name, err)
	}

	if len(nodeNames) == 0 {
		m.Logger.Info("no nodes matched selector, skipping hook", "name", hook.Name)
		return nil
	}

	var pendingNodes []hookJobSettings
	for _, nodeName := range nodeNames {
		perNode := hook
		perNode.Name = perNodeJobName(hook.Name, nodeName)
		perNode.ResolvedNodeName = nodeName

		resolved, err := m.resolveHookFromConfigMap(ctx, perNode)
		if err != nil {
			return err
		}

		m.logMutationStatus(obj, planName, resolved.Name, resolved.Event, resolved.Mutable, resolved.Checksum)

		skip, reason := hookSkipReason(obj, planName, resolved.Name, resolved.Event, resolved.Mutable, resolved.Checksum)
		if skip {
			m.Logger.Info("per-node hook job skipped", "name", resolved.Name, "node", nodeName, "reason", reason)
			continue
		}

		m.Logger.Info("per-node hook job will run", "name", resolved.Name, "node", nodeName, "reason", reason)
		pendingNodes = append(pendingNodes, resolved)
	}

	if len(pendingNodes) == 0 {
		return nil
	}

	// execute all node jobs in parallel — results collected by index for deterministic order
	results := make([]ebsv1alpha1.RestoreHookStatus, len(pendingNodes))
	g, gctx := errgroup.WithContext(ctx)

	for i, perNode := range pendingNodes {
		g.Go(func() error {
			hookStatus, err := m.executeHookJob(gctx, perNode)
			results[i] = hookStatus
			return err
		})
	}

	waitErr := g.Wait()

	// write statuses sequentially after all jobs complete —
	// deterministic order, no concurrent patch conflicts on the CR status
	for _, hookStatus := range results {
		if writeErr := m.Status.SetHooksRestoreFromPlan(ctx, obj, planName, hookStatus); writeErr != nil {
			m.Logger.Error(writeErr, "failed to update per-node hook status", "name", hookStatus.Name)
		}
	}

	return waitErr
}

// ensureHookConfigMap creates or updates a ConfigMap that stores the original
// Command and Args for this hook. For non-mutable hooks the ConfigMap is
// immutable — preventing args changes in spec from taking effect until the
// CR is recreated. For mutable hooks it is updated when args change.
func (m *ManageHooks) ensureHookConfigMap(ctx context.Context, h hookJobSettings) error {
	existing := &v1.ConfigMap{}
	err := m.Client.Get(ctx, types.NamespacedName{
		Name:      h.Name,
		Namespace: h.Namespace,
	}, existing)

	if apierrors.IsNotFound(err) {
		return m.createHookConfigMap(ctx, h)
	}

	if err != nil {
		return fmt.Errorf("get hook configmap %s: %w", h.Name, err)
	}

	// non-mutable — immutable ConfigMap, nothing to update
	if !h.Mutable {
		return nil
	}

	// mutable — update only if args changed
	var storedCommand, storedArgs []string
	if err := json.Unmarshal([]byte(existing.Data["command"]), &storedCommand); err != nil {
		return fmt.Errorf("unmarshal stored command for hook configmap %s: %w", h.Name, err)
	}

	if err := json.Unmarshal([]byte(existing.Data["args"]), &storedArgs); err != nil {
		return fmt.Errorf("unmarshal stored args for hook configmap %s: %w", h.Name, err)
	}

	if hookChecksum(h.Command, h.Args) == hookChecksum(storedCommand, storedArgs) {
		return nil
	}

	patch := client.MergeFrom(existing.DeepCopy())
	commandJSON, err := json.Marshal(h.Command)
	if err != nil {
		return fmt.Errorf("marshal command for hook configmap %s: %w", h.Name, err)
	}

	argsJSON, err := json.Marshal(h.Args)
	if err != nil {
		return fmt.Errorf("marshal args for hook configmap %s: %w", h.Name, err)
	}

	existing.Data["command"] = string(commandJSON)
	existing.Data["args"] = string(argsJSON)

	if err := m.Client.Patch(ctx, existing, patch); err != nil {
		return fmt.Errorf("patch hook configmap %s: %w", h.Name, err)
	}

	return nil
}

func (m *ManageHooks) createHookConfigMap(ctx context.Context, h hookJobSettings) error {
	commandJSON, err := json.Marshal(h.Command)
	if err != nil {
		return fmt.Errorf("marshal command for hook configmap %s: %w", h.Name, err)
	}

	argsJSON, err := json.Marshal(h.Args)
	if err != nil {
		return fmt.Errorf("marshal args for hook configmap %s: %w", h.Name, err)
	}

	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:            h.Name,
			Namespace:       h.Namespace,
			OwnerReferences: []metav1.OwnerReference{h.OwnerMeta},
		},
		Data: map[string]string{
			"command": string(commandJSON),
			"args":    string(argsJSON),
		},
	}

	if err := m.Client.Create(ctx, cm); err != nil {
		return fmt.Errorf("create hook configmap %s: %w", h.Name, err)
	}

	return nil
}

// readHookConfigMap reads Command and Args from the hook's ConfigMap
// and returns them along with the computed checksum.
// Returns NotFound error if the ConfigMap does not exist yet.
func (m *ManageHooks) readHookConfigMap(ctx context.Context, h hookJobSettings) (command, args []string, checksum string, err error) {
	cm := &v1.ConfigMap{}
	if err = m.Client.Get(ctx, types.NamespacedName{
		Name:      h.Name,
		Namespace: h.Namespace,
	}, cm); err != nil {
		return nil, nil, "", fmt.Errorf("get hook configmap %s: %w", h.Name, err)
	}

	if err = json.Unmarshal([]byte(cm.Data["command"]), &command); err != nil {
		return nil, nil, "", fmt.Errorf("unmarshal command from hook configmap %s: %w", h.Name, err)
	}

	if err = json.Unmarshal([]byte(cm.Data["args"]), &args); err != nil {
		return nil, nil, "", fmt.Errorf("unmarshal args from hook configmap %s: %w", h.Name, err)
	}

	checksum = hookChecksum(command, args)
	return
}

func (m *ManageHooks) resolveHookFromConfigMap(ctx context.Context, hook hookJobSettings) (hookJobSettings, error) {
	if hook.Mutable {
		return hook, nil
	}

	command, args, checksum, err := m.readHookConfigMap(ctx, hook)
	if err != nil && !apierrors.IsNotFound(err) {
		return hook, fmt.Errorf("read hook configmap %s: %w", hook.Name, err)
	}
	if err == nil {
		hook.Command = command
		hook.Args = args
		hook.Checksum = checksum
	}

	return hook, nil
}
