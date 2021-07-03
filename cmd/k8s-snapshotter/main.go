package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/evindunn/k8s-snapshotter/internal/k8s"
	"github.com/evindunn/k8s-snapshotter/internal/utils"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"os"
	"path"
)

func main() {
	var storageClassName string
	var isInCluster bool
	var createdByLabelValue string
	var deleteSnapsOlderThanDays int

	flag.StringVar(
		&storageClassName,
		"storageClassName",
		"",
		"The name of the storage class to query volumes from",
	)
	flag.BoolVar(
		&isInCluster,
		"inCluster",
		false,
		"(optional) whether running within a kubernetes cluster. Defaults to false.",
	)
	flag.StringVar(
		&createdByLabelValue,
		"createdByLabel",
		path.Base(os.Args[0]),
		"(optional) a Kubernetes label to use for identifying VolumeSnapshots created by this app. " +
			"Defaults to the name of this binary.",
	)
	flag.IntVar(
		&deleteSnapsOlderThanDays,
		"createdDays",
		7,
		"The number of days after which snapshots will be deleted by this app. Defaults to 7.",
	)

	flag.Parse()

	if storageClassName == "" {
		flag.Usage()
		os.Exit(1)
	}

	kubeConfig, err := k8s.GetDefaultKubeConfig(isInCluster)
	utils.CatchError(err)

	clientSet, err := kubernetes.NewForConfig(kubeConfig)
	utils.CatchError(err)

	namespaces, err := clientSet.CoreV1().Namespaces().List(context.TODO(), metaV1.ListOptions{})
	utils.CatchError(err)

	namespacePVCMap := make(map[string][]coreV1.PersistentVolumeClaim)

	for _, namespace := range namespaces.Items {
		pvcs, err := k8s.GetNamespacedPVCs(clientSet, namespace.Name, storageClassName)
		utils.CatchError(err)

		// If the namespace doesn't have any, don't include it in the map
		if len(pvcs) > 0 {
			namespacePVCMap[namespace.Name] = pvcs
		}
	}

	csiClient, err := csiV1.NewForConfig(kubeConfig)
	utils.CatchError(err)

	for namespace, pvcs := range namespacePVCMap {
		for _, pvc := range pvcs {
			log.Printf("[%s] Snapshotting volume '%s'\n", namespace, pvc.Name)

			snapshotName, err := k8s.CreateVolumeSnapshot(csiClient, namespace, pvc.Name)
			utils.CatchError(err)

			// Wait for snapshot to be ready
			log.Printf(
				"[%s] Waiting for snapshot '%s' to be ready...\n",
				namespace,
				snapshotName,
			)
			snapshot, err := k8s.WaitUntilSnapshotReady(csiClient, namespace, snapshotName)
			utils.CatchError(err)

			snapshotPVCName, err := k8s.CreatePVCFromSnapshot(
				clientSet,
				snapshotName,
				namespace,
				snapshot.Status.RestoreSize.String(),
			)
			utils.CatchError(err)

			log.Printf(
				"[%s] Volume '%s' created from snapshot '%s'\n",
				namespace,
				snapshotPVCName,
				snapshot.Name,
			)

			// Wait for snapshot volume to be ready
			log.Printf(
				"[%s] Waiting for volume '%s' to be ready...\n",
				namespace,
				snapshotPVCName,
			)
			_, err = k8s.WaitForPVCReady(clientSet, namespace, snapshotPVCName)
			utils.CatchError(err)

			log.Printf("[%s] Deleting snapshot '%s'\n", namespace, snapshotName)
			err = csiClient.SnapshotV1().VolumeSnapshots(namespace).Delete(
				context.TODO(),
				snapshotName,
				metaV1.DeleteOptions{},
			)
			utils.CatchError(err)

			log.Printf("[%s] Creating a backup job for volume '%s'\n", namespace, snapshotPVCName)
			jobName := fmt.Sprintf("backup-%s", snapshotPVCName)
			err = k8s.CreateBackupJob(
				clientSet,
				jobName,
				namespace,
				snapshotPVCName,
			)
			utils.CatchError(err)

			// Wait for job to complete
			// Wait for snapshot volume to be ready
			log.Printf(
				"[%s] Waiting for backup job '%s' to complete...\n",
				namespace,
				jobName,
			)
			job, err := k8s.WaitForJobReady(clientSet, namespace, jobName)
			utils.CatchError(err)

			if job.Status.Failed == 0 {
				log.Printf(
					"[%s] Backup job '%s' to completed successfully\n",
					namespace,
					jobName,
				)

				log.Printf("[%s] Deleting backup job '%s'\n", namespace, jobName)
				err = clientSet.BatchV1().Jobs(namespace).Delete(
					context.TODO(),
					jobName,
					metaV1.DeleteOptions{},
				)
				utils.CatchError(err)

				log.Printf("[%s] Deleting volume '%s'\n", namespace, snapshotPVCName)
				err = clientSet.CoreV1().PersistentVolumeClaims(namespace).Delete(
					context.TODO(),
					snapshotPVCName,
					metaV1.DeleteOptions{},
				)
				utils.CatchError(err)

			} else {
				log.Printf(
					"[%s] One or more pods for backup job '%s' failed\n",
					namespace,
					jobName,
				)
			}
		}
		fmt.Println()
	}
}