package k8s

import (
	"context"
	"fmt"
	snapshotV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"strings"
	"time"
)

/*
CreateVolumeSnapshot creates a VolumeSnapshot from the given PersistentVolumeClaim name in
the given namespace with the given labels
 */
func CreateVolumeSnapshot(clientSet *csiV1.Clientset, namespace string, pvc string) (string, error) {
	now := time.Now().Unix()
	var snapshot snapshotV1.VolumeSnapshot

	snapshotValues := SnapshotValues{
		SnapshotName: fmt.Sprintf("%d-%s", now, pvc),
		Namespace:    namespace,
		PVCName:      pvc,
	}

	err := ParseK8STemplate(
		SnapshotTemplate,
		"volumeSnapshot",
		&snapshotValues,
		&snapshot,
	)

	if err != nil {
		return "", err
	}

	_, err = clientSet.SnapshotV1().VolumeSnapshots(namespace).Create(
		context.TODO(),
		&snapshot,
		metaV1.CreateOptions{},
	)

	if err != nil {
		return "", err
	}

	return snapshot.Name, nil
}

/*
GetNamespacedVolumeSnapshotsOlderThan returns a list of VolumeSnapshots for the given namespace
matching the given labels older than the given number of days
*/
func GetNamespacedVolumeSnapshotsOlderThan(clientSet *csiV1.Clientset, namespace string, labels map[string]string, days *metaV1.Time) ([]string, error) {
	var snapshots []string
	var labelList []string

	for labelName, labelValue := range labels {
		labelList = append(labelList, fmt.Sprintf("%s=%s", labelName, labelValue))
	}

	allNamespaceSnapshots, err := clientSet.SnapshotV1().VolumeSnapshots(namespace).List(
		context.TODO(),
		metaV1.ListOptions{
			LabelSelector: strings.Join(labelList, ","),
		},
	)

	if err != nil {
		return nil, err
	}

	for _, snapshot := range allNamespaceSnapshots.Items {
		creationTimestamp := snapshot.ObjectMeta.CreationTimestamp
		if creationTimestamp.Before(days) {
			snapshots = append(snapshots, snapshot.Name)
		}
	}

	return snapshots, nil
}
