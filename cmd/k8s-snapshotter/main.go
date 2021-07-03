package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/evindunn/k8s-snapshotter/internal/k8s"
	"github.com/evindunn/k8s-snapshotter/internal/utils"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"golang.org/x/sync/errgroup"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"os"
)

func main() {
	var storageClassName string
	var isInCluster bool
	var errorGroup errgroup.Group

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

	flag.Parse()

	if storageClassName == "" {
		flag.Usage()
		os.Exit(1)
	}

	kubeConfig, err := k8s.GetDefaultKubeConfig(isInCluster)
	utils.CatchError(err)

	clientSet, err := kubernetes.NewForConfig(kubeConfig)
	utils.CatchError(err)

	csiClient, err := csiV1.NewForConfig(kubeConfig)
	utils.CatchError(err)

	namespaces, err := clientSet.CoreV1().Namespaces().List(context.TODO(), metaV1.ListOptions{})
	utils.CatchError(err)

	for _, namespace := range namespaces.Items {
		err = backupNamespace(&errorGroup, clientSet, csiClient, namespace.Name, storageClassName)
		utils.CatchError(err)
	}

	if err = errorGroup.Wait(); err != nil {
		log.Fatalln(err)
	} else {
		log.Println("Backup complete")
		log.Println()
	}
}

/*
backupNamespace backs up all PersistentVolumes in the given namespace with the given StorageClass name
*/
func backupNamespace(errorGroup *errgroup.Group, clientSet *kubernetes.Clientset, csiClient *csiV1.Clientset, namespace string, storageClassName string) error {
	pvcNames, err := k8s.GetNamespacedPVCs(clientSet, namespace, storageClassName)
	if err != nil {
		return err
	}

	if len(pvcNames) > 0 {
		log.Printf("[%s] Starting namespace backup\n", namespace)
	}

	for idx, _ := range pvcNames {
		pvcName := pvcNames[idx]
		// TODO: This dies on the first error; Instead we want to log the error & continue the other routines
		errorGroup.Go(func() error {
			return backupNamespacedPVC(clientSet, csiClient, namespace, pvcName)
		})
	}

	return nil
}

/*
backupNamespacedPVC backs up the given PersistentVolumeClaim in the given namespace
*/
func backupNamespacedPVC(clientSet *kubernetes.Clientset, csiClient *csiV1.Clientset, namespace string, pvcName string) error {
	log.Printf("[%s] Snapshotting volume '%s'\n", namespace, pvcName)

	// Create the snapshot
	snapshotName, err := k8s.CreateVolumeSnapshot(csiClient, namespace, pvcName)
	if err != nil {
		return err
	}

	log.Printf(
		"[%s] Waiting for snapshot '%s' to be ready...\n",
		namespace,
		snapshotName,
	)
	snapshot, err := k8s.WaitUntilSnapshotReady(csiClient, namespace, snapshotName)
	if err != nil {
		return err
	}

	// Create a new PVC from the snapshot
	snapshotPVCName, err := k8s.CreatePVCFromSnapshot(
		clientSet,
		snapshotName,
		namespace,
		snapshot.Status.RestoreSize.String(),
	)
	if err != nil {
		return err
	}

	log.Printf(
		"[%s] Volume '%s' created from snapshot '%s'\n",
		namespace,
		snapshotPVCName,
		snapshot.Name,
	)

	log.Printf(
		"[%s] Waiting for volume '%s' to be ready...\n",
		namespace,
		snapshotPVCName,
	)
	_, err = k8s.WaitForPVCReady(clientSet, namespace, snapshotPVCName)
	if err != nil {
		return err
	}

	// Delete the snapshot, we've now copied its data to a PVC
	log.Printf("[%s] Deleting snapshot '%s'\n", namespace, snapshotName)
	err = csiClient.SnapshotV1().VolumeSnapshots(namespace).Delete(
		context.TODO(),
		snapshotName,
		metaV1.DeleteOptions{},
	)
	if err != nil {
		return err
	}

	// Create a backup job for the created PVC
	log.Printf("[%s] Creating a backup job for volume '%s'\n", namespace, snapshotPVCName)
	jobName := fmt.Sprintf("backup-%s", snapshotPVCName)
	err = k8s.CreateBackupJob(
		clientSet,
		jobName,
		namespace,
		snapshotPVCName,
	)
	if err != nil {
		return err
	}

	log.Printf(
		"[%s] Waiting for backup job '%s' to complete...\n",
		namespace,
		jobName,
	)
	job, err := k8s.WaitForJobReady(clientSet, namespace, jobName)
	if err != nil {
		return err
	}

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
		if err != nil {
			return err
		}

		log.Printf("[%s] Deleting volume '%s'\n", namespace, snapshotPVCName)
		err = clientSet.CoreV1().PersistentVolumeClaims(namespace).Delete(
			context.TODO(),
			snapshotPVCName,
			metaV1.DeleteOptions{},
		)
		if err != nil {
			return err
		}

	} else {
		log.Printf(
			"[%s] One or more pods for backup job '%s' failed\n",
			namespace,
			jobName,
		)
	}

	return nil
}