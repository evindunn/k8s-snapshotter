package k8s

import (
	"context"
	"fmt"
	snapshotV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"time"
)

const snapshotWaitTime = 2

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
WaitUntilSnapshotReady waits until the given VolumeSnapshot name in the given namespace is "ReadyToUse"
 */
func WaitUntilSnapshotReady(csiClient *csiV1.Clientset, namespace string, snapshotName string) (*snapshotV1.VolumeSnapshot, error) {
	var snapshot *snapshotV1.VolumeSnapshot
	var err error

	for {
		snapshot, err = csiClient.SnapshotV1().VolumeSnapshots(namespace).Get(
			context.TODO(),
			snapshotName,
			metaV1.GetOptions{},
		)
		if err != nil {
			return nil, err
		}

		if snapshot.Status != nil && *snapshot.Status.ReadyToUse {
			break
		}
		time.Sleep(snapshotWaitTime * time.Second)
	}

	return snapshot, err
}
