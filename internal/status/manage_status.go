package status

import (
	"context"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
)

// ManageStatus updates CR status via the status subresource.
type ManageStatus struct {
	Client client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger
}

// New returns a new status manager.
func New(c client.Client, r *runtime.Scheme, l logr.Logger) *ManageStatus {
	return &ManageStatus{Client: c, Scheme: r, Logger: l.WithName("Status")}
}

// cmp options for comparing LinkerdTrustRotationStatus objects.
// We ignore volatile fields like LastUpdated to avoid infinite patches.
func statusCmpOptions() []cmp.Option {
	return []cmp.Option{
		cmpopts.EquateEmpty(),
		cmpopts.IgnoreFields(ebsv1alpha1.EBSSnapshotRestoreStatus{}, "LastUpdated"),
	}
}

// Patch mutates the status with the provided function and patches it using MergeFrom.
// Caller must pass a live object (fetched from the API).
func (m *ManageStatus) Patch(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, processName string, mutate func(st *ebsv1alpha1.EBSSnapshotRestoreStatus)) error {
	// Base for merge patch
	oldObj := obj.DeepCopy()

	// Deep snapshot of BEFORE
	beforePtr := obj.Status.DeepCopy()
	after := *beforePtr.DeepCopy()

	mutate(&after)

	// Compare with cmp, ignoring volatile fields
	if cmp.Equal(*beforePtr, after, statusCmpOptions()...) {
		// No meaningful change — skip patch
		return nil
	}

	// Apply mutated status and set LastUpdated only when there are changes
	now := metav1.NewTime(time.Now().UTC())
	after.LastUpdated = &now
	obj.Status = after

	return m.Client.Status().Patch(ctx, obj, client.MergeFrom(oldObj))
}

func (m *ManageStatus) SetInitStatusRestorePlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore) error {
	return m.Patch(ctx, obj, "SetValidateRestorePlan", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		if st.RestorePlans == nil {
			st.RestorePlans = make(map[string]ebsv1alpha1.RestorePlanStatus)
		}

		st.Phase = ebsv1alpha1.PhaseIdle
	})
}

// SetPhase sets the high-level phase, with optional reason/message.
func (m *ManageStatus) SetPhase(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore,
	phase ebsv1alpha1.RestorePhase) error {
	return m.Patch(ctx, obj, "SetPhase", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		st.Phase = phase
	})
}

// SetActivePlan sets the high-level phase, with optional reason/message.
func (m *ManageStatus) SetActivePlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, activePlan string) error {
	return m.Patch(ctx, obj, "SetActivePlan", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		st.ActivePlan = activePlan
	})
}

func (m *ManageStatus) SetValidateRestorePlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore,
	planName, snapshotRestoreTime string, lock bool, clusters, operators []ebsv1alpha1.RestoreTargetStatus) error {
	return m.Patch(ctx, obj, "SetValidateRestorePlan", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		if st.RestorePlans == nil {
			st.RestorePlans = make(map[string]ebsv1alpha1.RestorePlanStatus)
		}

		existsPlan := st.RestorePlans[planName]
		existsPlan.Lock = lock
		existsPlan.SnapshotRestoreTime = snapshotRestoreTime
		existsPlan.Clusters = clusters
		existsPlan.Operators = operators
		st.RestorePlans[planName] = existsPlan
	})
}

func (m *ManageStatus) SetScaleRestorePlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore,
	planName string, countScaleUp int32, clusters, operators []ebsv1alpha1.RestoreTargetStatus) error {
	return m.Patch(ctx, obj, "SetScaleRestorePlan", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		if st.RestorePlans == nil {
			st.RestorePlans = make(map[string]ebsv1alpha1.RestorePlanStatus)
		}

		existsPlan := st.RestorePlans[planName]
		existsPlan.Clusters = clusters
		existsPlan.Operators = operators
		existsPlan.CountScaleUp = countScaleUp
		st.RestorePlans[planName] = existsPlan
	})
}

func (m *ManageStatus) SetRestoreFromPlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore,
	planName string, lock bool, clusters, operators []ebsv1alpha1.RestoreTargetStatus) error {
	return m.Patch(ctx, obj, "SetRestoreFromPlan", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		if st.RestorePlans == nil {
			st.RestorePlans = make(map[string]ebsv1alpha1.RestorePlanStatus)
		}

		existsPlan := st.RestorePlans[planName]
		existsPlan.Clusters = clusters
		existsPlan.Operators = operators
		existsPlan.Lock = lock
		st.RestorePlans[planName] = existsPlan
	})
}

func (m *ManageStatus) SetHooksRestoreFromPlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore,
	planName string, hook ebsv1alpha1.RestoreHookStatus) error {
	return m.Patch(ctx, obj, "SetHooksFromPlan", func(st *ebsv1alpha1.EBSSnapshotRestoreStatus) {
		if st.RestorePlans == nil {
			st.RestorePlans = make(map[string]ebsv1alpha1.RestorePlanStatus)
		}

		plan := st.RestorePlans[planName]
		for i, h := range plan.Hooks {
			if h.Name == hook.Name && h.Event == hook.Event {
				plan.Hooks[i] = hook
				st.RestorePlans[planName] = plan
				return
			}
		}

		plan.Hooks = append(plan.Hooks, hook)
		st.RestorePlans[planName] = plan
	})
}
