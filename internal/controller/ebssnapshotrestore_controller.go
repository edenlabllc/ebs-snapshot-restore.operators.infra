/*
Copyright 2026 Edenlab.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"ebs-snapshot-restore.operators.infra/internal/restore"
	"ebs-snapshot-restore.operators.infra/internal/scale"
	"ebs-snapshot-restore.operators.infra/internal/status"
	"ebs-snapshot-restore.operators.infra/internal/validate"
	"fmt"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"time"

	ebsv1alpha1 "ebs-snapshot-restore.operators.infra/api/v1alpha1"
)

// EBSSnapshotRestoreReconciler reconciles a EBSSnapshotRestore object
type EBSSnapshotRestoreReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

const (
	frequency = time.Second * 30
)

// +kubebuilder:rbac:groups=ebs.aws.edenlab.io,resources=ebssnapshotrestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=ebs.aws.edenlab.io,resources=ebssnapshotrestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ebs.aws.edenlab.io,resources=ebssnapshotrestores/finalizers,verbs=update

func (r *EBSSnapshotRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	reqLogger := logf.FromContext(ctx)
	statusMgr := status.New(r.Client, r.Scheme, reqLogger)
	validateMgr := validate.New(r.Client, r.Scheme, reqLogger, statusMgr)
	restoreMgr := restore.New(r.Client, r.Scheme, reqLogger, statusMgr)
	scaleMgr := scale.New(r.Client, r.Scheme, reqLogger, statusMgr)

	eSR := &ebsv1alpha1.EBSSnapshotRestore{}
	if err := r.Client.Get(ctx, req.NamespacedName, eSR); err != nil {
		if apierrors.IsNotFound(err) {
			reqLogger.Error(nil, fmt.Sprintf("Can not find CRD by name: %s", req.Name))
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, err
	}

	if err := statusMgr.SetInitStatusRestorePlan(ctx, eSR); err != nil {
		return ctrl.Result{}, err
	}

	if err := statusMgr.SetPhase(ctx, eSR, ebsv1alpha1.PhaseValidating); err != nil {
		return ctrl.Result{}, err
	}

	if err := validateMgr.ValidateListSnapshots(ctx, eSR); err != nil {
		return ctrl.Result{}, err
	}

	if err := statusMgr.SetPhase(ctx, eSR, ebsv1alpha1.PhaseScalingDown); err != nil {
		return ctrl.Result{}, err
	}

	if err := scaleMgr.ScaleRestorePlan(ctx, eSR, scale.DownScale); err != nil {
		return ctrl.Result{}, err
	}

	if err := statusMgr.SetPhase(ctx, eSR, ebsv1alpha1.PhaseRestoring); err != nil {
		return ctrl.Result{}, err
	}

	if err := restoreMgr.RestoreFromPlan(ctx, eSR); err != nil {
		return ctrl.Result{}, err
	}

	if err := statusMgr.SetPhase(ctx, eSR, ebsv1alpha1.PhaseScalingUp); err != nil {
		return ctrl.Result{}, err
	}

	if err := scaleMgr.ScaleRestorePlan(ctx, eSR, scale.UpScale); err != nil {
		return ctrl.Result{}, err
	}

	if err := statusMgr.SetPhase(ctx, eSR, ebsv1alpha1.PhaseCompleted); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: frequency}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EBSSnapshotRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&ebsv1alpha1.EBSSnapshotRestore{}).
		WithEventFilter(predicate.GenerationChangedPredicate{}).
		Named("ebssnapshotrestore").
		Complete(r)
}
