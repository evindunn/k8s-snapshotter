package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/evindunn/k8s-snapshotter/internal/k8s"
	"github.com/evindunn/k8s-snapshotter/internal/utils"
	snapshotV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/apis/volumesnapshot/v1"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	batchV1 "k8s.io/api/batch/v1"
	coreV1 "k8s.io/api/core/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"os"
	"path"
	"time"
)

// CreatedByLabelName Follows the convention at
// https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
const CreatedByLabelName = "app.kubernetes.io/created-by"

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

	snapshotLabels := make(map[string]string)
	snapshotLabels[CreatedByLabelName] = createdByLabelValue

	// TODO: Re-enable the configurable number of days
	// xDaysAgoNanos := time.Hour * 24 * -time.Duration(deleteSnapsOlderThanDays)
	// xDaysAgoNanos := time.Duration(-10)
	// xDaysAgo := metaV1.NewTime(time.Now().Add(xDaysAgoNanos))

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
			var snapshot *snapshotV1.VolumeSnapshot
			var snapshotPVC *coreV1.PersistentVolumeClaim
			var job *batchV1.Job

			log.Printf("Snapshotting '%s' in namespace '%s'\n", pvc.Name, namespace)

			snapshotName, err := k8s.CreateVolumeSnapshot(csiClient, namespace, pvc.Name)
			utils.CatchError(err)

			// Wait for snapshot to be ready
			log.Printf(
				"Waiting for snapshot '%s' in namespace '%s' to be ready...\n",
				snapshotName,
				namespace,
			)
			for {
				snapshot, err = csiClient.SnapshotV1().VolumeSnapshots(namespace).Get(
					context.TODO(),
					snapshotName,
					metaV1.GetOptions{},
				)
				if err != nil {
					utils.CatchError(err)
				}

				if snapshot.Status != nil && *snapshot.Status.ReadyToUse {
					break
				}
				time.Sleep(time.Second)
			}

			snapshotPVCName, err := k8s.CreatePVCFromSnapshot(
				clientSet,
				snapshotName,
				namespace,
				snapshot.Status.RestoreSize.String(),
			)
			utils.CatchError(err)

			log.Printf(
				"Volume '%s' created from snapshot '%s' in namespace '%s'\n",
				snapshotPVCName,
				snapshot.Name,
				namespace,
			)

			// Wait for snapshot volume to be ready
			log.Printf(
				"Waiting for volume '%s' in namespace '%s' to be ready...\n",
				snapshotPVCName,
				namespace,
			)
			for {
				snapshotPVC, err = clientSet.CoreV1().PersistentVolumeClaims(namespace).Get(
					context.TODO(),
					snapshotPVCName,
					metaV1.GetOptions{},
				)
				utils.CatchError(err)

				if snapshotPVC.Status.Phase == "Bound" {
					break
				}

				time.Sleep(time.Second)
			}

			log.Printf("Deleting snapshot '%s' in namespace '%s'\n", snapshotName, namespace)
			err = csiClient.SnapshotV1().VolumeSnapshots(namespace).Delete(
				context.TODO(),
				snapshotName,
				metaV1.DeleteOptions{},
			)
			utils.CatchError(err)

			log.Printf("Creating a backup job for volume '%s'\n", snapshotPVCName)
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
				"Waiting for backup job '%s' in namespace '%s' to complete...\n",
				jobName,
				namespace,
			)
			for {
				job, err = clientSet.BatchV1().Jobs(namespace).Get(
					context.TODO(),
					jobName,
					metaV1.GetOptions{},
				)
				utils.CatchError(err)

				if job.Status.Active == 0 {
					break
				}

				time.Sleep(time.Second)
			}

			if job.Status.Failed == 0 {
				log.Printf(
					"Backup job '%s' in namespace '%s' to completed successfully\n",
					jobName,
					namespace,
				)

				log.Printf("Deleting backup job '%s' in namespace '%s'\n", jobName, namespace)
				err = clientSet.BatchV1().Jobs(namespace).Delete(
					context.TODO(),
					jobName,
					metaV1.DeleteOptions{},
				)
				utils.CatchError(err)

				log.Printf("Deleting volume '%s' in namespace '%s'\n", snapshotPVCName, namespace)
				err = clientSet.CoreV1().PersistentVolumeClaims(namespace).Delete(
					context.TODO(),
					snapshotPVCName,
					metaV1.DeleteOptions{},
				)
				utils.CatchError(err)

			} else {
				log.Printf(
					"One or more pods for backup job '%s' in namespace '%s' to failed\n",
					jobName,
					namespace,
				)
			}
		}
		fmt.Println()
	}
}