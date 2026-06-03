package scale

import (
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/net/context"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
	"ebs-snapshot-restore.operators.infra/internal/status"
)

const (
	pollInterval               = 5 * time.Second
	pollTimeout                = 10 * time.Minute
	stsPollInterval            = 2 * time.Second
	stsPollTimeout             = 10 * time.Second
	scaleUpOperatorWaitTimeout = 20 * time.Second

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
			if planStatus.Lock && scale == DownScale {
				continue
			}

			if planStatus.Lock && planStatus.CountScaleUp > 0 && scale == UpScale {
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
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, 0, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}

			if err := m.processTargets(ctx, plan.Operators, scaleOperatorsStatus, m.scaleOperatorUp, UpScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, 0, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}

			if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, 1, scaleClustersStatus, scaleOperatorsStatus); err != nil {
				return err
			}
		case DownScale:
			if err := m.processTargets(ctx, plan.Operators, scaleOperatorsStatus, m.scaleOperatorDown, DownScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, 0, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}

			if err := m.processTargets(ctx, plan.Clusters, scaleClustersStatus, m.scaleClusterDown, DownScale); err != nil {
				if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, 0, scaleClustersStatus, scaleOperatorsStatus); err != nil {
					return err
				}

				if err := m.Status.SetPhase(ctx, obj, ebsv1alpha1.PhaseFailed); err != nil {
					return err
				}

				return err
			}

			if err := m.Status.SetScaleRestorePlan(ctx, obj, planName, 0, scaleClustersStatus, scaleOperatorsStatus); err != nil {
				return err
			}
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

	if cluster.SkipWaitScaleDown {
		m.Logger.Info("Skipping StatefulSet scale-down wait",
			"cluster", cluster.Name,
			"readyReplicas", sts.Status.ReadyReplicas,
			"expectedReplicas", 0,
		)

		return nil
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
	if cluster.SkipWaitScaleUp && cluster.ParallelPodManagement {
		return fmt.Errorf("skipWaitScaleUp and parallelPodManagement cannot be used together")
	}

	sts := &appsv1.StatefulSet{}
	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return err
	}

	if cluster.ParallelPodManagement && sts.Spec.PodManagementPolicy == appsv1.OrderedReadyPodManagement &&
		(sts.Spec.Replicas == nil || *sts.Spec.Replicas < cluster.Replicas) {
		if err := m.recreateSTSWithPodManagementPolicy(ctx, sts, appsv1.ParallelPodManagement); err != nil {
			return err
		}
	}

	if sts.Spec.Replicas == nil || *sts.Spec.Replicas < cluster.Replicas {
		baseSTS := client.MergeFrom(sts.DeepCopy())
		sts.Spec.Replicas = &cluster.Replicas
		if err := m.Client.Patch(ctx, sts, baseSTS); err != nil {
			return err
		}
	}

	if cluster.SkipWaitScaleUp {
		m.Logger.Info("Skipping StatefulSet scale-up wait",
			"cluster", cluster.Name,
			"readyReplicas", sts.Status.ReadyReplicas,
			"expectedReplicas", cluster.Replicas,
		)

		return nil
	}

	if err := wait.PollUntilContextTimeout(ctx, pollInterval, pollTimeout, true,
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

			switch sts.Spec.UpdateStrategy.Type {
			case appsv1.RollingUpdateStatefulSetStrategyType:
				if sts.Status.ReadyReplicas == cluster.Replicas &&
					sts.Status.UpdatedReplicas == cluster.Replicas &&
					sts.Status.CurrentRevision == sts.Status.UpdateRevision {
					return true, nil
				}
			case appsv1.OnDeleteStatefulSetStrategyType:
				if sts.Status.ReadyReplicas == cluster.Replicas &&
					sts.Status.Replicas == cluster.Replicas &&
					sts.Status.ObservedGeneration >= sts.Generation {
					return true, nil
				}
			default:
				err := fmt.Errorf("unknown update strategy type: %s", sts.Spec.UpdateStrategy.Type)
				m.Logger.Error(err, "not possible to scale up", "type", sts.Spec.UpdateStrategy.Type)

				return false, err
			}

			return false, nil
		},
	); err != nil {
		return err
	}

	if err := m.Client.Get(ctx, types.NamespacedName{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
	}, sts); err != nil {
		return err
	}
	if cluster.ParallelPodManagement && sts.Spec.PodManagementPolicy == appsv1.ParallelPodManagement {
		if err := m.recreateSTSWithPodManagementPolicy(ctx, sts, appsv1.OrderedReadyPodManagement); err != nil {
			return err
		}
	}

	time.Sleep(scaleUpOperatorWaitTimeout)

	return nil
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

	if operator.SkipWaitScaleDown {
		m.Logger.Info("Skipping Deployment scale-down wait",
			"cluster", operator.Name,
			"readyReplicas", deployment.Status.ReadyReplicas,
			"expectedReplicas", 0,
		)

		return nil
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

	if deployment.Spec.Replicas == nil || *deployment.Spec.Replicas < operator.Replicas {
		baseDeployment := client.MergeFrom(deployment.DeepCopy())
		deployment.Spec.Replicas = &operator.Replicas
		if err := m.Client.Patch(ctx, deployment, baseDeployment); err != nil {
			return err
		}
	}

	if operator.SkipWaitScaleUp {
		m.Logger.Info("Skipping Deployment scale-up wait",
			"cluster", operator.Name,
			"readyReplicas", deployment.Status.ReadyReplicas,
			"expectedReplicas", operator.Replicas,
		)

		return nil
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

func (m *ManageScale) recreateSTSWithPodManagementPolicy(ctx context.Context, sts *appsv1.StatefulSet, podManagementPolicy appsv1.PodManagementPolicyType) error {
	m.Logger.Info("Recreating StatefulSet with new pod management policy",
		"name", sts.Name,
		"namespace", sts.Namespace,
		"podManagementPolicy", podManagementPolicy,
	)

	deletePolicy := metav1.DeletePropagationOrphan
	if err := m.Client.Delete(ctx, sts, &client.DeleteOptions{PropagationPolicy: &deletePolicy}); err != nil {
		return fmt.Errorf("failed to delete STS: %w", err)
	}

	if err := wait.PollUntilContextTimeout(ctx, stsPollInterval, stsPollTimeout, true,
		func(ctx context.Context) (bool, error) {
			existing := &appsv1.StatefulSet{}
			err := m.Client.Get(ctx, types.NamespacedName{
				Name:      sts.Name,
				Namespace: sts.Namespace,
			}, existing)
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			return false, err
		},
	); err != nil {
		return fmt.Errorf("timed out waiting for STS deletion: %w", err)
	}

	newSTS := sts.DeepCopy()
	newSTS.ResourceVersion = ""
	newSTS.UID = ""
	newSTS.Spec.PodManagementPolicy = podManagementPolicy

	if err := m.Client.Create(ctx, newSTS); err != nil {
		return fmt.Errorf("failed to recreate STS: %w", err)
	}

	m.Logger.Info("StatefulSet recreated with policy"+fmt.Sprintf(" [%s]", podManagementPolicy), "name", sts.Name)
	return nil
}
