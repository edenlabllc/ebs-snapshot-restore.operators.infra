package validate

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-logr/logr"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
	"ebs-snapshot-restore.operators.infra/internal/status"
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
			if planStatus.Lock {
				lock = planStatus.Lock
				plan.SnapshotRestoreTime = planStatus.SnapshotRestoreTime
			}
		}

		switch {
		case len(plan.SnapshotRestoreTime) == 0:
			latestTime, clustersStatus, err := m.findLatestSnapshotTime(ctx, planName, plan.Clusters)
			if err != nil {
				if setErr := m.failPlan(ctx, obj, planName, plan.SnapshotRestoreTime, lock, clustersStatus, nil); setErr != nil {
					return setErr
				}

				lastErr = err
				continue
			}

			plan.SnapshotRestoreTime = latestTime
			fallthrough
		case len(plan.SnapshotRestoreTime) > 0:
			if err := validateSnapshotTime(plan.SnapshotRestoreTime); err != nil {
				clustersStatus := buildFailedClustersStatus(plan.Clusters, err)
				if setErr := m.failPlan(ctx, obj, planName, plan.SnapshotRestoreTime, lock, clustersStatus, nil); setErr != nil {
					return setErr
				}

				lastErr = err
				continue
			}

			clustersStatus, operatorsStatus, err := m.validateSnapshot(ctx, planName, &plan)
			if err != nil {
				if setErr := m.failPlan(ctx, obj, planName, plan.SnapshotRestoreTime, lock, clustersStatus, operatorsStatus); setErr != nil {
					return setErr
				}

				lastErr = err
				continue
			}

			if setErr := m.Status.SetValidateRestorePlan(ctx, obj, planName, plan.SnapshotRestoreTime, lock,
				clustersStatus, operatorsStatus); setErr != nil {
				return setErr
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

func (m *ManageValidate) fetchClusterResources(ctx context.Context, cluster ebsv1alpha1.RestoreTarget, planName, snapshotTime string) (*v1.PersistentVolumeClaimList, *snapv1.VolumeSnapshotList, error) {
	if cluster.ClaimSelector == nil {
		return nil, nil, fmt.Errorf("cluster %s has no claimSelector", cluster.Name)
	}

	pvcSelector, err := metav1.LabelSelectorAsSelector(cluster.ClaimSelector)
	if err != nil {
		return nil, nil, err
	}

	pvcList := &v1.PersistentVolumeClaimList{}
	if err := m.Client.List(ctx, pvcList, client.InNamespace(cluster.Namespace), client.MatchingLabelsSelector{Selector: pvcSelector}); err != nil {
		return nil, nil, err
	}

	if len(pvcList.Items) == 0 {
		return nil, nil, fmt.Errorf("no PVCs found for cluster %s in namespace %s by selector %s, check claimSelector in plan %s",
			cluster.Name, cluster.Namespace, cluster.ClaimSelector.MatchLabels, planName)
	}

	listOpts := []client.ListOption{client.InNamespace(cluster.Namespace)}
	if snapshotTime != "" {
		listOpts = append(listOpts, client.MatchingLabels{"snapscheduler.backube/when": snapshotTime})
	}

	vsList := &snapv1.VolumeSnapshotList{}
	if err := m.Client.List(ctx, vsList, listOpts...); err != nil {
		return nil, nil, err
	}

	return pvcList, vsList, nil
}

func (m *ManageValidate) failPlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, planName string,
	snapshotTime string, lock bool, clustersStatus []ebsv1alpha1.RestoreTargetStatus,
	operatorsStatus []ebsv1alpha1.RestoreTargetStatus) error {
	if setErr := m.Status.SetValidateRestorePlan(ctx, obj, planName, snapshotTime, lock, clustersStatus, operatorsStatus); setErr != nil {
		return setErr
	}

	if setErr := m.Status.SetActivePlan(ctx, obj, planName); setErr != nil {
		return setErr
	}

	return m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed)
}

func (m *ManageValidate) findLatestSnapshotTime(ctx context.Context, planName string, clusters []ebsv1alpha1.RestoreTarget) (string, []ebsv1alpha1.RestoreTargetStatus, error) {
	var (
		latestTime     string
		clustersStatus []ebsv1alpha1.RestoreTargetStatus
	)

	for _, cluster := range clusters {
		clusterStatus := ebsv1alpha1.RestoreTargetStatus{
			Name:             cluster.Name,
			Namespace:        cluster.Namespace,
			Type:             cluster.Type,
			OriginalReplicas: cluster.Replicas,
			CurrentReplicas:  cluster.Replicas,
		}

		pvcList, vsList, err := m.fetchClusterResources(ctx, cluster, planName, "")
		if err != nil {
			return "", appendFailed(clustersStatus, clusterStatus, err), err
		}

		clusterLatestTime := ""
		for _, pvc := range pvcList.Items {
			prefix := pvc.Name + "-" + planName + "-"
			for _, vs := range vsList.Items {
				if !strings.HasPrefix(vs.Name, prefix) {
					continue
				}

				when := strings.TrimPrefix(vs.Name, prefix)
				if when > clusterLatestTime {
					clusterLatestTime = when
				}
			}
		}

		if clusterLatestTime == "" {
			err := fmt.Errorf("no snapshots found for cluster %s in plan %s", cluster.Name, planName)
			return "", appendFailed(clustersStatus, clusterStatus, err), err
		}

		if clusterLatestTime > latestTime {
			latestTime = clusterLatestTime
		}

		clustersStatus = append(clustersStatus, clusterStatus)
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

		sts := &appsv1.StatefulSet{}
		if err := m.Client.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, sts); err != nil {
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		clusterStatus.OriginalReplicas = cluster.Replicas
		if sts.Spec.Replicas != nil {
			clusterStatus.CurrentReplicas = *sts.Spec.Replicas
		}

		if sts.Spec.PodManagementPolicy != "" {
			clusterStatus.PodManagementPolicy = sts.Spec.PodManagementPolicy
		}

		pvcList, vsList, err := m.fetchClusterResources(ctx, cluster, planName, plan.SnapshotRestoreTime)
		if err != nil {
			return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
		}

		for _, vs := range vsList.Items {
			for _, pvc := range pvcList.Items {
				if pvc.Name+"-"+planName+"-"+plan.SnapshotRestoreTime == vs.Name {
					if vs.Status == nil || vs.Status.ReadyToUse == nil || !*vs.Status.ReadyToUse {
						err := fmt.Errorf("snapshot %s is not ready to use", vs.Name)
						return appendFailed(validateClustersStatus, clusterStatus, err), validateOperatorsStatus, err
					}

					snapshotsRef = append(snapshotsRef, ebsv1alpha1.RestoreSnapshotRef{
						SnapshotUID:      string(vs.GetUID()),
						SnapshotName:     vs.GetName(),
						PVCName:          pvc.Name,
						SkipRestoringPVC: slices.Contains(cluster.SkipRestoringPVCs, pvc.Name),
					})
				}
			}
		}

		if len(snapshotsRef) == 0 {
			err := fmt.Errorf("no snapshots found in namespace %s by time %s for plan %s, check snapshotRestoreTime",
				cluster.Namespace, plan.SnapshotRestoreTime, planName)
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

func buildFailedClustersStatus(clusters []ebsv1alpha1.RestoreTarget, err error) []ebsv1alpha1.RestoreTargetStatus {
	statuses := make([]ebsv1alpha1.RestoreTargetStatus, 0, len(clusters))
	for _, cluster := range clusters {
		statuses = appendFailed(statuses, ebsv1alpha1.RestoreTargetStatus{
			Name:             cluster.Name,
			Namespace:        cluster.Namespace,
			Type:             cluster.Type,
			CurrentReplicas:  cluster.Replicas,
			OriginalReplicas: cluster.Replicas,
		}, err)
	}

	return statuses
}

func validateSnapshotTime(s string) error {
	_, err := time.Parse(snapshotTimeLayout, s)
	if err != nil {
		return fmt.Errorf("invalid snapshotRestoreTime format, expected YYYYMMDDHHMM: %w", err)
	}

	return nil
}
