package validate

import (
	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
	"ebs-snapshot-restore.operators.infra/internal/status"
	"fmt"
	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"strings"
	"time"
)

const (
	snapshotTimeLayout = "200601021504"
)

type ManageValidate struct {
	Client client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger
	Status *status.ManageStatus
}

func New(c client.Client, s *runtime.Scheme, l logr.Logger, status *status.ManageStatus) *ManageValidate {
	return &ManageValidate{Client: c, Scheme: s, Logger: l.WithName("Validate"), Status: status}
}

func (m *ManageValidate) ValidateListSnapshots(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore) error {
	var (
		lastErr error
		lock    bool
	)

	for planName, plan := range obj.Spec.RestorePlans {
		m.Logger.Info("Plan", "name", planName)

		if planStatus, ok := obj.Status.RestorePlans[planName]; ok {
			if planStatus.Lock && (plan.Lock == nil || *plan.Lock) {
				continue
			}

			if plan.Lock != nil && !*plan.Lock {
				lock = *plan.Lock
			}
		}

		switch {
		case len(plan.SnapshotRestoreTime) == 0:
			latestTime, clustersStatus, err := m.findLatestSnapshotTime(ctx, planName, plan.Clusters)
			if err != nil {
				if setErr := m.Status.SetValidateRestorePlan(ctx, obj, planName, "",
					lock, clustersStatus, nil); setErr != nil {
					return setErr
				}

				if setErr := m.Status.SetActivePlan(ctx, obj, planName); setErr != nil {
					return setErr
				}

				if setErr := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); setErr != nil {
					return setErr
				}

				lastErr = err
				continue
			}

			plan.SnapshotRestoreTime = latestTime
			fallthrough
		case len(plan.SnapshotRestoreTime) > 0:
			if err := validateSnapshotTime(plan.SnapshotRestoreTime); err != nil {
				return err
			}

			clustersStatus, operatorsStatus, err := m.validateSnapshot(ctx, planName, &plan)
			if setErr := m.Status.SetValidateRestorePlan(ctx, obj, planName, plan.SnapshotRestoreTime, lock,
				clustersStatus, operatorsStatus); setErr != nil {
				return setErr
			}

			if err != nil {
				if setErr := m.Status.SetActivePlan(ctx, obj, planName); setErr != nil {
					return setErr
				}

				if setErr := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); setErr != nil {
					return setErr
				}

				lastErr = err
				continue
			}

			if lastErr == nil {
				if setErr := m.Status.SetActivePlan(ctx, obj, planName); setErr != nil {
					return setErr
				}
			}
		}
	}

	return lastErr
}

func validateSnapshotTime(s string) error {
	_, err := time.Parse(snapshotTimeLayout, s)
	if err != nil {
		return fmt.Errorf("invalid snapshotRestoreTime format, expected YYYYMMDDHHMM: %w", err)
	}

	return nil
}

func (m *ManageValidate) findLatestSnapshotTime(ctx context.Context, planName string, clusters []ebsv1alpha1.RestoreTarget) (string, []ebsv1alpha1.RestoreTargetStatus, error) {
	var (
		latestTime     string
		clustersStatus []ebsv1alpha1.RestoreTargetStatus
	)

	for _, cluster := range clusters {
		clusterStatus := ebsv1alpha1.RestoreTargetStatus{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Type:      cluster.Type,
		}

		if cluster.ClaimSelector == nil {
			err := fmt.Errorf("cluster %s has no claimSelector", cluster.Name)
			return "", appendFailed(clustersStatus, clusterStatus, err), err
		}

		pvcSelector, err := metav1.LabelSelectorAsSelector(cluster.ClaimSelector)
		if err != nil {
			return "", appendFailed(clustersStatus, clusterStatus, err), err
		}

		pvcList := &v1.PersistentVolumeClaimList{}
		if err := m.Client.List(ctx, pvcList, client.InNamespace(cluster.Namespace), client.MatchingLabelsSelector{Selector: pvcSelector}); err != nil {
			return "", appendFailed(clustersStatus, clusterStatus, err), err
		}

		vsList := &snapv1.VolumeSnapshotList{}
		if err := m.Client.List(ctx, vsList, client.InNamespace(cluster.Namespace)); err != nil {
			return "", appendFailed(clustersStatus, clusterStatus, err), err
		}

		for _, pvc := range pvcList.Items {
			prefix := pvc.Name + "-" + planName + "-"
			for _, vs := range vsList.Items {
				if !strings.HasPrefix(vs.Name, prefix) {
					continue
				}

				when := strings.TrimPrefix(vs.Name, prefix)
				if when > latestTime {
					latestTime = when
				}
			}
		}

		clusterStatus.Phase = ebsv1alpha1.PhaseValidating
		clustersStatus = append(clustersStatus, clusterStatus)
	}

	if latestTime == "" {
		err := fmt.Errorf("no snapshots found for plan %s", planName)
		return "", clustersStatus, err
	}

	return latestTime, clustersStatus, nil
}

func (m *ManageValidate) validateSnapshot(ctx context.Context, planName string,
	plan *ebsv1alpha1.RestorePlan) ([]ebsv1alpha1.RestoreTargetStatus, []ebsv1alpha1.RestoreTargetStatus, error) {
	var (
		validateClustersStatus  []ebsv1alpha1.RestoreTargetStatus
		validateOperatorsStatus []ebsv1alpha1.RestoreTargetStatus
	)

	for _, cluster := range plan.Clusters {
		var snapshotsRef []ebsv1alpha1.RestoreSnapshotRef
		clusterStatus := ebsv1alpha1.RestoreTargetStatus{
			Name:      cluster.Name,
			Namespace: cluster.Namespace,
			Type:      cluster.Type,
		}

		if cluster.ClaimSelector == nil {
			err := fmt.Errorf("cluster %s has no claimSelector", cluster.Name)
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		sts := &appsv1.StatefulSet{}
		if err := m.Client.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, sts); err != nil {
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		clusterStatus.OriginalReplicas = cluster.Replicas
		if sts.Spec.Replicas != nil {
			clusterStatus.CurrentReplicas = *sts.Spec.Replicas
		}

		pvcList := &v1.PersistentVolumeClaimList{}
		pvcSelector, err := metav1.LabelSelectorAsSelector(cluster.ClaimSelector)
		if err != nil {
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		if err := m.Client.List(ctx, pvcList, client.InNamespace(cluster.Namespace), client.MatchingLabelsSelector{Selector: pvcSelector}); err != nil {
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		vsList := &snapv1.VolumeSnapshotList{}
		if err := m.Client.List(ctx, vsList, client.InNamespace(cluster.Namespace),
			client.MatchingLabels{"snapscheduler.backube/when": plan.SnapshotRestoreTime}); err != nil {
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		for _, vs := range vsList.Items {
			for _, pvc := range pvcList.Items {
				if pvc.Name+"-"+planName+"-"+plan.SnapshotRestoreTime == vs.Name {
					if !*vs.Status.ReadyToUse {
						err := fmt.Errorf("snapshot %s is not ready to use", vs.Name)
						return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
					}

					snapshotsRef = append(snapshotsRef, ebsv1alpha1.RestoreSnapshotRef{
						SnapshotUID:  string(vs.GetUID()),
						SnapshotName: vs.GetName(),
						PVCName:      pvc.Name,
					})
				}
			}
		}

		if len(snapshotsRef) == 0 {
			err := fmt.Errorf("snapshots for plan %s is not found by time %s", planName, plan.SnapshotRestoreTime)
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		clusterStatus.SnapshotsRef = snapshotsRef
		clusterStatus.Phase = ebsv1alpha1.PhaseValidated
		validateClustersStatus = append(validateClustersStatus, clusterStatus)
	}

	for _, operator := range plan.Operators {
		operatorStatus := ebsv1alpha1.RestoreTargetStatus{
			Name:      operator.Name,
			Namespace: operator.Namespace,
			Type:      operator.Type,
		}

		deployment := &appsv1.Deployment{}
		if err := m.Client.Get(ctx, types.NamespacedName{Name: operator.Name, Namespace: operator.Namespace}, deployment); err != nil {
			return validateClustersStatus, appendFailed(validateOperatorsStatus, operatorStatus, err), err
		}

		operatorStatus.OriginalReplicas = operator.Replicas
		if deployment.Spec.Replicas != nil {
			operatorStatus.CurrentReplicas = *deployment.Spec.Replicas
		}

		operatorStatus.Phase = ebsv1alpha1.PhaseValidated
		validateOperatorsStatus = append(validateOperatorsStatus, operatorStatus)
	}

	return validateClustersStatus, validateOperatorsStatus, nil
}

func appendFailed(statuses []ebsv1alpha1.RestoreTargetStatus, base ebsv1alpha1.RestoreTargetStatus, err error) []ebsv1alpha1.RestoreTargetStatus {
	base.Phase = ebsv1alpha1.PhaseFailed
	base.Error = err.Error()
	return append(statuses, base)
}
