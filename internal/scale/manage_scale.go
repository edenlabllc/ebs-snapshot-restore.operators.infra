package scale

import (
	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
	"ebs-snapshot-restore.operators.infra/internal/status"
	"github.com/go-logr/logr"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

const (
	pollInterval = 5 * time.Second
	pollTimeout  = 10 * time.Minute

	UpScale   = "up"
	DownScale = "down"
)

type ManageScale struct {
	Client client.Client
	Scheme *runtime.Scheme
	Logger logr.Logger
	Status *status.ManageStatus
}

func New(c client.Client, s *runtime.Scheme, l logr.Logger, status *status.ManageStatus) *ManageScale {
	return &ManageScale{Client: c, Scheme: s, Logger: l.WithName("Scale"), Status: status}
}

func (m *ManageScale) processTargets(ctx context.Context,
	targets []ebsv1alpha1.RestoreTarget,
	status []ebsv1alpha1.RestoreTargetStatus,
	scaleFunc func(context.Context, *ebsv1alpha1.RestoreTarget) error,
	scale string,
) error {
	for i := range targets {
		t := &targets[i]

		if err := scaleFunc(ctx, t); err != nil {
			status[i].Phase = ebsv1alpha1.PhaseFailed
			status[i].Error = err.Error()
			return err
		}

		switch scale {
		case UpScale:
			status[i].CurrentReplicas = status[i].OriginalReplicas
			status[i].Phase = ebsv1alpha1.PhaseCompleted
		case DownScale:
			status[i].CurrentReplicas = 0
			status[i].Phase = ebsv1alpha1.PhaseScaledDown
		}
	}

	return nil
}

func (m *ManageScale) ScaleRestorePlan(ctx context.Context, obj *ebsv1alpha1.EBSSnapshotRestore, scale string) error {
	for planName, plan := range obj.Spec.RestorePlans {
		var (
			scaleClustersStatus  []ebsv1alpha1.RestoreTargetStatus
			scaleOperatorsStatus []ebsv1alpha1.RestoreTargetStatus
		)

		m.Logger.Info("Plan", "name", planName)
		if err := m.Status.SetActivePlan(ctx, obj, planName); err != nil {
			return err
		}

		if planStatus, ok := obj.Status.RestorePlans[planName]; ok {
			if planStatus.Lock && (plan.Lock == nil || *plan.Lock) && scale == DownScale {
				continue
			}

			scaleClustersStatus = make([]ebsv1alpha1.RestoreTargetStatus, len(planStatus.Clusters))
			copy(scaleClustersStatus, planStatus.Clusters)

			scaleOperatorsStatus = make([]ebsv1alpha1.RestoreTargetStatus, len(planStatus.Operators))
			copy(scaleOperatorsStatus, planStatus.Operators)
		}

		switch scale {
		case UpScale:
			if err := m.processTargets(ctx, plan.Clusters, scaleClustersStatus, m.scaleClusterUp, UpScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}

			if err := m.processTargets(ctx, plan.Operators, scaleOperatorsStatus, m.scaleOperatorUp, UpScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}
		case DownScale:
			if err := m.processTargets(ctx, plan.Operators, scaleOperatorsStatus, m.scaleOperatorDown, DownScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}

			if err := m.processTargets(ctx, plan.Clusters, scaleClustersStatus, m.scaleClusterDown, DownScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}
		}

		if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, scaleClustersStatus, scaleOperatorsStatus); err != nil {
			return err
		}
	}

	return nil
}

func (m *ManageScale) scaleClusterDown(ctx context.Context, cluster *ebsv1alpha1.RestoreTarget) error {
	sts := &appsv1.StatefulSet{}
	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return err
	}

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != 0 {
		zero := int32(0)
		baseSTS := client.MergeFrom(sts.DeepCopy())
		sts.Spec.Replicas = &zero
		if err := m.Client.Patch(ctx, sts, baseSTS); err != nil {
			return err
		}
	}

	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			if err := m.Client.Get(ctx, types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			}, sts); err != nil {
				return false, err
			}

			m.Logger.Info("Waiting for StatefulSet to be scale down",
				"name", cluster.Name,
				"ready", sts.Status.ReadyReplicas,
				"expected", 0,
			)

			if sts.Status.Replicas == 0 && sts.Status.ReadyReplicas == 0 {
				return true, nil
			}

			return false, nil
		},
	)
}

func (m *ManageScale) scaleClusterUp(ctx context.Context, cluster *ebsv1alpha1.RestoreTarget) error {
	sts := &appsv1.StatefulSet{}
	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return err
	}

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != cluster.Replicas {
		baseSTS := client.MergeFrom(sts.DeepCopy())
		sts.Spec.Replicas = &cluster.Replicas
		if err := m.Client.Patch(ctx, sts, baseSTS); err != nil {
			return err
		}
	}

	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			if err := m.Client.Get(ctx, types.NamespacedName{
				Name:      cluster.Name,
				Namespace: cluster.Namespace,
			}, sts); err != nil {
				return false, err
			}

			m.Logger.Info("Waiting for StatefulSet to be ready",
				"name", cluster.Name,
				"ready", sts.Status.ReadyReplicas,
				"expected", cluster.Replicas,
			)

			if sts.Status.ReadyReplicas == cluster.Replicas &&
				sts.Status.UpdatedReplicas == cluster.Replicas &&
				sts.Status.CurrentRevision == sts.Status.UpdateRevision {
				return true, nil
			}

			return false, nil
		},
	)
}

func (m *ManageScale) scaleOperatorDown(ctx context.Context, operator *ebsv1alpha1.RestoreTarget) error {
	deployment := &appsv1.Deployment{}
	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      operator.Name,
		Namespace: operator.Namespace,
	}, deployment); err != nil {
		return err
	}

	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != 0 {
		zero := int32(0)
		baseDeployment := client.MergeFrom(deployment.DeepCopy())
		deployment.Spec.Replicas = &zero
		if err := m.Client.Patch(ctx, deployment, baseDeployment); err != nil {
			return err
		}
	}

	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			if err := m.Client.Get(ctx, types.NamespacedName{
				Name:      operator.Name,
				Namespace: operator.Namespace,
			}, deployment); err != nil {
				return false, err
			}

			m.Logger.Info("Waiting for Deployment to be scale down",
				"name", operator.Name,
				"ready", deployment.Status.ReadyReplicas,
				"expected", 0,
			)

			if deployment.Status.Replicas == 0 &&
				deployment.Status.ReadyReplicas == 0 &&
				deployment.Status.AvailableReplicas == 0 {
				return true, nil
			}

			return false, nil
		},
	)
}

func (m *ManageScale) scaleOperatorUp(ctx context.Context, operator *ebsv1alpha1.RestoreTarget) error {
	deployment := &appsv1.Deployment{}
	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      operator.Name,
		Namespace: operator.Namespace,
	}, deployment); err != nil {
		return err
	}

	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas != operator.Replicas {
		baseDeployment := client.MergeFrom(deployment.DeepCopy())
		deployment.Spec.Replicas = &operator.Replicas
		if err := m.Client.Patch(ctx, deployment, baseDeployment); err != nil {
			return err
		}
	}

	return wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
		func(ctx context.Context) (bool, error) {
			if err := m.Client.Get(ctx, types.NamespacedName{
				Name:      operator.Name,
				Namespace: operator.Namespace,
			}, deployment); err != nil {
				return false, err
			}

			m.Logger.Info("Waiting for Deployment to be ready",
				"name", operator.Name,
				"ready", deployment.Status.ReadyReplicas,
				"expected", operator.Replicas,
			)

			if deployment.Status.ReadyReplicas == operator.Replicas &&
				deployment.Status.UpdatedReplicas == operator.Replicas &&
				deployment.Status.AvailableReplicas == operator.Replicas {
				return true, nil
			}

			return false, nil
		},
	)
}
