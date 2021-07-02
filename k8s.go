package main

import (
	"context"
	"fmt"
	snapshotV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path/filepath"
	"time"
)

/**
createVolumeSnapshot creates a VolumeSnapshot from the given PersistentVolumeClaim name in
the given namespace
 */
func createVolumeSnapshot(clientSet *csiV1.Clientset, namespace string, pvc string) (*snapshotV1.VolumeSnapshot, error) {
	today := time.Now().Format("2006-01-02")
	snapshotName := fmt.Sprintf("%s-%s", today, pvc)

	snapshotSrc := snapshotV1.VolumeSnapshot{
		TypeMeta:   metaV1.TypeMeta{},
		ObjectMeta: metaV1.ObjectMeta{
			Name: snapshotName,
			Namespace: namespace,
		},
		Spec:       snapshotV1.VolumeSnapshotSpec{
			Source: snapshotV1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &pvc,
			},
		},
	}

	snapshot, err := clientSet.SnapshotV1().VolumeSnapshots(namespace).Create(
		context.TODO(),
		&snapshotSrc,
		metaV1.CreateOptions{},
	)

	if err != nil {
		return nil, err
	}

	return snapshot, nil
}

/**
getNamespacedPVCs returns a list of bound PersistentVolumeClaims for the given namespace and storage class
 */
func getNamespacedPVCs(clientSet *kubernetes.Clientset, namespace string, storageClassName string) ([]string, error) {
	var cephPVCs []string

	allNamespacePVCs, err := clientSet.CoreV1().PersistentVolumeClaims(namespace).List(
		context.TODO(),
		metaV1.ListOptions{},
	)
	if err != nil {
		return nil, err
	}

	for _, pvc := range allNamespacePVCs.Items {
		isBound := pvc.Status.Phase == "Bound"
		isCorrectStorageClass := *pvc.Spec.StorageClassName == storageClassName

		if isBound && isCorrectStorageClass {
			cephPVCs = append(cephPVCs, pvc.Name)
		}
	}

	return cephPVCs, nil
}

/*
getDefaultKubeConfig returns a k8s Config based on the pod's service account if inCluster.
If not inCluster, returns a Config based on ~/.kube/config
*/
func getDefaultKubeConfig(inCluster bool) (*rest.Config, error) {
	if inCluster {
		kubeConfig, err := rest.InClusterConfig()
		if err != nil {
			return &rest.Config{}, err
		}
		return kubeConfig, nil
	}

	homeDir := homedir.HomeDir()
	kubeConfigFile := filepath.Join(
		homeDir,
		".kube",
		"config",
	)
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigFile)
	if err != nil {
		return &rest.Config{}, err
	}

	return kubeConfig, nil
}
