package controllers

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/rabbitmq/cluster-operator/v2/api/v1beta1"
	"github.com/rabbitmq/cluster-operator/v2/internal/status"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
)

const beforeZeroReplicasConfigured = "rabbitmq.com/before-zero-replicas-configured"

func (r *RabbitmqClusterReconciler) scaleToZero(current, sts *appsv1.StatefulSet) bool {
	currentReplicas := *current.Spec.Replicas
	desiredReplicas := *sts.Spec.Replicas

	if desiredReplicas == 0 && currentReplicas > 0 {
		return true
	}

	return false
}

func (r *RabbitmqClusterReconciler) scaleFromZero(current, sts *appsv1.StatefulSet) bool {
	currentReplicas := *current.Spec.Replicas
	desiredReplicas := *sts.Spec.Replicas

	if currentReplicas == 0 && desiredReplicas > 0 {
		return true
	}

	return false
}

func (r *RabbitmqClusterReconciler) saveReplicasBeforeZero(ctx context.Context, cluster *v1beta1.RabbitmqCluster, current *appsv1.StatefulSet) error {
	var err error
	currentReplicas := *current.Spec.Replicas
	logger := ctrl.LoggerFrom(ctx)
	msg := "Cluster Scale down to 0 replicas."
	reason := "ScaleDownToZero"
	logger.Info(msg)
	if _, exists := cluster.Annotations[beforeZeroReplicasConfigured]; !exists {
		err = r.updateAnnotation(ctx, cluster, cluster.Namespace, cluster.Name, beforeZeroReplicasConfigured, fmt.Sprint(currentReplicas))
	}
	r.Recorder.Event(cluster, corev1.EventTypeNormal, reason, msg)
	return err
}

func (r *RabbitmqClusterReconciler) removeReplicasBeforeZero(ctx context.Context, cluster *v1beta1.RabbitmqCluster, sts *appsv1.StatefulSet) error {
	var err error
	var beforeZeroReplicas int64
	logger := ctrl.LoggerFrom(ctx)
	desiredReplicas := *sts.Spec.Replicas
	if _, ok := cluster.Annotations[beforeZeroReplicasConfigured]; !ok {
		return errors.New("rabbitmq.com/before-zero-replicas-configured annotation not found")
	}

	beforeZeroReplicas, err = strconv.ParseInt(cluster.Annotations[beforeZeroReplicasConfigured], 10, 32)
	if err != nil {
		msg := "Failed to convert string to integer for before-zero-replicas-configuration annotation"
		reason := "TransformErrorOperation"
		logger.Error(errors.New(reason), msg)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, msg)
		cluster.Status.SetCondition(status.ReconcileSuccess, corev1.ConditionFalse, reason, msg)
		if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
			logger.Error(statusErr, "Failed to update ReconcileSuccess condition state")
		}
		return err
	}

	if desiredReplicas < int32(beforeZeroReplicas) {
		msg := fmt.Sprintf("Cluster Scale down not supported; tried to scale cluster from %d nodes to %d nodes", int32(beforeZeroReplicas), desiredReplicas)
		reason := "UnsupportedOperation"
		logger.Error(errors.New(reason), msg)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, msg)
		cluster.Status.SetCondition(status.ReconcileSuccess, corev1.ConditionFalse, reason, msg)
		if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
			logger.Error(statusErr, "Failed to update ReconcileSuccess condition state")
		}
		return errors.New(msg)
	}

	err = r.deleteAnnotation(ctx, cluster, beforeZeroReplicasConfigured)
	return err

}
