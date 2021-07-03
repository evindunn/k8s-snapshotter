package k8s

import (
	"context"
	"fmt"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
)

/*
GetNamespacedPVCs returns a list of bound PersistentVolumeClaims for the given namespace and storage class
*/
func GetNamespacedPVCs(clientSet *kubernetes.Clientset, namespace string, storageClassName string) ([]coreV1.PersistentVolumeClaim, error) {
	var cephPVCs []coreV1.PersistentVolumeClaim

	allNamespacePVCs, err := clientSet.CoreV1().PersistentVolumeClaims(namespace).List(
		context.TODO(),
		metaV1.ListOptions{
			LabelSelector: fmt.Sprintf("%s!=%s", createdByLabelName, createdByLabelValue),
		},
	)
	if err != nil {
		return nil, err
	}

	// TODO: Skip PVCs created by this app
	for _, pvc := range allNamespacePVCs.Items {
		isBound := pvc.Status.Phase == "Bound"
		isCorrectStorageClass := *pvc.Spec.StorageClassName == storageClassName

		if isBound && isCorrectStorageClass {
			cephPVCs = append(cephPVCs, pvc)
		}
	}

	return cephPVCs, nil
}

/*
CreatePVCFromSnapshot uses the given pvcValues to create a PersistentVolumeClaim from
a VolumeSnapshot
 */
func CreatePVCFromSnapshot(clientSet *kubernetes.Clientset, snapshotName string, namespace string, pvcSize string) (string, error) {
	var pvcRequest coreV1.PersistentVolumeClaim

	pvcLabels := make(map[string]string)
	pvcLabels[createdByLabelName] = createdByLabelValue
	pvcLabels["snapshotName"] = snapshotName

	pvcValues := PVCFromSnapshotValues{
		SnapshotName: snapshotName,
		PVCLabels:    pvcLabels,
		PVCSize:      pvcSize,
		Namespace:    namespace,
	}

	err := ParseK8STemplate(
		VolumeFromSnapshotTemplate,
		"volumeSnapshotPVC",
		pvcValues,
		&pvcRequest,
	)

	if err != nil {
		return "", err
	}

	pvc, err := clientSet.CoreV1().PersistentVolumeClaims(pvcValues.Namespace).Create(
		context.TODO(),
		&pvcRequest,
		metaV1.CreateOptions{},
	)

	if err != nil {
		return "", err
	}

	return pvc.Name, nil
}

func WaitForPVCReady(clientSet *kubernetes.Clientset, namespace string, pvcName string) (*coreV1.PersistentVolumeClaim, error) {
	var pvc *coreV1.PersistentVolumeClaim
	var err error

	for {
		pvc, err = clientSet.CoreV1().PersistentVolumeClaims(namespace).Get(
			context.TODO(),
			pvcName,
			metaV1.GetOptions{},
		)
		if err != nil {
			return nil, err
		}

		if pvc.Status.Phase == "Bound" {
			break
		}

		time.Sleep(time.Second)
	}

	return pvc, nil
}