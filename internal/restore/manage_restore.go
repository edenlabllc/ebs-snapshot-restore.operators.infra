package restore

import (
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/net/context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
	"ebs-snapshot-restore.operators.infra/internal/status"
)

const (
	pollInterval = 5 * time.Second
	pollTimeout  = 10 * time.Minute
)

type ManageRestore struct {
	Client client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger
	Status *status.ManageStatus
}

func New(c client.Client, s *runtime.Scheme, l logr.Logger, status *status.ManageStatus) *ManageRestore {
	return &ManageRestore{Client: c, Scheme: s, Logger: l.WithName("Restore"), Status: status}
}

func (m *ManageRestore) RestoreFromPlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore) error {
	for planStatusName, planStatus := range obj.Status.RestorePlans {
		m.Logger.Info("Plan", "name", planStatusName)
		if err := m.Status.SetActivePlan(ctx, obj, planStatusName); err != nil {
			return err
		}

		if plan, ok := obj.Spec.RestorePlans[planStatusName]; ok {
			if planStatus.Lock && (plan.Lock == nil || *plan.Lock) {
				continue
			}
		}

		clusters := make([]ebsv1alpha1.RestoreTargetStatus, len(planStatus.Clusters))
		copy(clusters, planStatus.Clusters)

		for key, cluster := range planStatus.Clusters {
			for _, snap := range cluster.SnapshotsRef {
				if err := m.restorePVC(ctx, cluster.Namespace, snap.PVCName, snap.SnapshotName); err != nil {
					clusters[key].Phase = ebsv1alpha1.PhaseFailed
					clusters[key].Error = err.Error()
					if err := m.Status.SetRestoreFromPlan(ctx, obj, planStatusName, false, clusters, planStatus.Operators); err != nil {
						return err
					}

					if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
						return err
					}

					return err
				}
			}

			clusters[key].Phase = ebsv1alpha1.PhaseRestored
		}

		if err := m.Status.SetRestoreFromPlan(ctx, obj, planStatusName, true, clusters, planStatus.Operators); err != nil {
			return err
		}
	}

	return nil
}

func (m *ManageRestore) restorePVC(ctx context.Context, namespace string, pvcName string, snapshotName string) error {
	existsPVC := &v1.PersistentVolumeClaim{}
	log := m.Logger.WithValues("pvc", pvcName, "ns", namespace)

	err := m.Client.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: namespace}, existsPVC)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	newPVC := buildPVCFromExisting(existsPVC, snapshotName)

	if err == nil {
		log.Info("Deleting existing PVC", "name", pvcName)

		if err := m.Client.Delete(ctx, existsPVC); err != nil {
			return err
		}

		if err := m.waitPVCDeleted(ctx, pvcName, namespace); err != nil {
			return err
		}
	}

	log.Info("Creating PVC from snapshot", "name", snapshotName)
	if err := m.Client.Create(ctx, newPVC); err != nil {
		return err
	}

	log.Info("Waiting for PV to be provisioned")
	pvName, err := m.waitPVProvisioned(ctx, pvcName, namespace, newPVC.UID)
	if err != nil {
		return err
	}

	log.Info("Patching PVC with VolumeName", "name", pvName)
	patch := client.MergeFrom(newPVC.DeepCopy())
	newPVC.Spec.VolumeName = pvName
	if err := m.Client.Patch(ctx, newPVC, patch); err != nil {
		return err
	}

	if err := m.waitPVCBound(ctx, pvcName, namespace); err != nil {
		return err
	}

	log.Info("PVC restored successfully")

	return nil
}

func buildPVCFromExisting(existsPVC *v1.PersistentVolumeClaim, snapshotName string) *v1.PersistentVolumeClaim {
	meta := *existsPVC.ObjectMeta.DeepCopy()

	meta.ResourceVersion = ""
	meta.UID = ""
	meta.ManagedFields = nil
	meta.CreationTimestamp = metav1.Time{}
	meta.Finalizers = nil
	meta.OwnerReferences = nil
	meta.Generation = 0

	pvc := &v1.PersistentVolumeClaim{
		ObjectMeta: meta,
		Spec: v1.PersistentVolumeClaimSpec{
			AccessModes:      existsPVC.Spec.AccessModes,
			StorageClassName: existsPVC.Spec.StorageClassName,
			Resources:        existsPVC.Spec.Resources,
			VolumeMode:       existsPVC.Spec.VolumeMode,
			DataSource: &v1.TypedLocalObjectReference{
				APIGroup: pointer.String("snapshot.storage.k8s.io"),
				Kind:     "VolumeSnapshot",
				Name:     snapshotName,
			},
		},
	}

	return pvc
}

func (m *ManageRestore) waitPVCDeleted(ctx context.Context, name, namespace string) error {
	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			var pvc v1.PersistentVolumeClaim

			err := m.Client.Get(ctx, types.NamespacedName{
				Name:      name,
				Namespace: namespace,
			}, &pvc)

			if errors.IsNotFound(err) {
				return true, nil
			}

			return false, err
		},
	)
}

func (m *ManageRestore) waitPVCBound(ctx context.Context, name, namespace string) error {
	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			var pvc v1.PersistentVolumeClaim

			if err := m.Client.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, &pvc); err != nil {
				return false, err
			}

			return pvc.Status.Phase == v1.ClaimBound, nil
		},
	)
}

func (m *ManageRestore) waitPVProvisioned(ctx context.Context, pvcName, namespace string, pvcUID types.UID) (string, error) {
	var pvName string
	return pvName, wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			pvList := &v1.PersistentVolumeList{}
			if err := m.Client.List(ctx, pvList); err != nil {
				return false, err
			}

			for _, pv := range pvList.Items {
				ref := pv.Spec.ClaimRef
				if ref != nil && ref.Name == pvcName && ref.Namespace == namespace && ref.UID == pvcUID {
					pvName = pv.Name
					return true, nil
				}
			}

			return false, nil
		},
	)
}
