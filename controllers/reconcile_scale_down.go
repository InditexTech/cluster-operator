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

const (
	beforeZeroReplicasConfigured = "rabbitmq.com/before-zero-replicas-configured"
)

// cluster scale down not supported
// log error, publish warning event, and set ReconcileSuccess to false when scale down request detected
func (r *RabbitmqClusterReconciler) scaleDown(ctx context.Context, cluster *v1beta1.RabbitmqCluster, current, sts *appsv1.StatefulSet) bool {
	logger := ctrl.LoggerFrom(ctx)

	currentReplicas := *current.Spec.Replicas
	desiredReplicas := *sts.Spec.Replicas

	if desiredReplicas == 0 {
		msg := "Cluster Scale down to 0 replicas."
		reason := "ScaleDownToZero"
		logger.Info(msg)
		if _, exists := cluster.Annotations[beforeZeroReplicasConfigured]; !exists {
			r.updateAnnotation(ctx, cluster, cluster.Namespace, cluster.Name, beforeZeroReplicasConfigured, fmt.Sprint(currentReplicas))
		}
		r.Recorder.Event(cluster, corev1.EventTypeNormal, reason, msg)
		return false
	}
	if currentReplicas == 0 && desiredReplicas > 0 {
		if v, ok := cluster.Annotations[beforeZeroReplicasConfigured]; ok {
			beforeZeroReplicas, err := strconv.Atoi(v)
			if err != nil {
				msg := "Failed to convert string to integer for before-zero-replicas-configuration annotation"
				reason := "TranformErrorOperation"
				logger.Error(errors.New(reason), msg)
			}

			if int32(beforeZeroReplicas) > desiredReplicas {
				msg := fmt.Sprintf("Cluster Scale down not supported; tried to scale cluster from %d nodes (configured before zero) to %d nodes", int32(beforeZeroReplicas), desiredReplicas)
				reason := "UnsupportedOperation"
				logger.Error(errors.New(reason), msg)
				r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, msg)
				cluster.Status.SetCondition(status.ReconcileSuccess, corev1.ConditionFalse, reason, msg)
				if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
					logger.Error(statusErr, "Failed to update ReconcileSuccess condition state")
				}
				return true
			}
		}
	}
	if currentReplicas > desiredReplicas {
		msg := fmt.Sprintf("Cluster Scale down not supported; tried to scale cluster from %d nodes to %d nodes", currentReplicas, desiredReplicas)
		reason := "UnsupportedOperation"
		logger.Error(errors.New(reason), msg)
		r.Recorder.Event(cluster, corev1.EventTypeWarning, reason, msg)
		cluster.Status.SetCondition(status.ReconcileSuccess, corev1.ConditionFalse, reason, msg)
		if statusErr := r.Status().Update(ctx, cluster); statusErr != nil {
			logger.Error(statusErr, "Failed to update ReconcileSuccess condition state")
		}
		return true
	}

	if _, ok := cluster.Annotations[beforeZeroReplicasConfigured]; ok && desiredReplicas > 0 {
		// if we have an annotation that indicates the cluster was configured before zero replicas,
		// we should remove it to avoid confusion in the future
		logger.Info("Removing annotation indicating cluster was configured before zero replicas")
		r.deleteAnnotation(ctx, cluster, beforeZeroReplicasConfigured)
	}

	return false
}
