package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/evindunn/k8s-snapshotter/internal/k8s"
	"github.com/evindunn/k8s-snapshotter/internal/utils"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"os"
	"sync"
)

func main() {
	var storageClassName string
	var isInCluster bool

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

	backupScheduler := utils.NewBackupJobScheduler(backupNamespacedPVC)

	for _, namespace := range namespaces.Items {
		err = backupNamespace(backupScheduler, clientSet, csiClient, namespace.Name, storageClassName)
		utils.CatchError(err)
	}

	success := true
	for err := range backupScheduler.Wait() {
		log.Printf("ERROR: %s\n", err)
		success = false
	}

	if success {
		log.Println("Backup completed successfully")
		fmt.Println()
	} else {
		log.Fatalln("Back failed with one or more errors")
	}
}

/*
backupNamespace backs up all PersistentVolumes in the given namespace with the given StorageClass name
*/
func backupNamespace(jobScheduler *utils.BackupJobScheduler, clientSet *kubernetes.Clientset, csiClient *csiV1.Clientset, namespace string, storageClassName string) error {
	pvcNames, err := k8s.GetNamespacedPVCs(clientSet, namespace, storageClassName)
	if err != nil {
		return err
	}

	if len(pvcNames) > 0 {
		log.Printf("[%s] Starting namespace backup\n", namespace)
	}

	for _, pvcName := range pvcNames {
		jobInput := utils.BackupJobInput{
			ClientSet: clientSet,
			CSIClient: csiClient,
			Namespace: namespace,
			PVCName:   pvcName,
		}
		jobScheduler.Schedule(&jobInput)
	}

	return nil
}

/*
backupNamespacedPVC backs up the given PersistentVolumeClaim in the given namespace
*/
func backupNamespacedPVC(wg *sync.WaitGroup, errorChan chan <- error, jobInput *utils.BackupJobInput) {
	defer wg.Done()

	log.Printf("[%s] Snapshotting volume '%s'\n", jobInput.Namespace, jobInput.PVCName)

	// Create the snapshot
	snapshotName, err := k8s.CreateVolumeSnapshot(jobInput.CSIClient, jobInput.Namespace, jobInput.PVCName)
	if err != nil {
		errorChan <- err
		return
	}

	log.Printf(
		"[%s] Waiting for snapshot '%s' to be ready...\n",
		jobInput.Namespace,
		snapshotName,
	)
	snapshot, err := k8s.WaitUntilSnapshotReady(jobInput.CSIClient, jobInput.Namespace, snapshotName)
	if err != nil {
		errorChan <- err
		return
	}

	// Create a new PVC from the snapshot
	snapshotPVCName, err := k8s.CreatePVCFromSnapshot(
		jobInput.ClientSet,
		snapshotName,
		jobInput.Namespace,
		snapshot.Status.RestoreSize.String(),
	)
	if err != nil {
		errorChan <- err
		return
	}

	log.Printf(
		"[%s] Volume '%s' created from snapshot '%s'\n",
		jobInput.Namespace,
		snapshotPVCName,
		snapshot.Name,
	)

	log.Printf(
		"[%s] Waiting for volume '%s' to be ready...\n",
		jobInput.Namespace,
		snapshotPVCName,
	)
	_, err = k8s.WaitForPVCReady(jobInput.ClientSet, jobInput.Namespace, snapshotPVCName)
	if err != nil {
		errorChan <- err
		return
	}

	// Delete the snapshot, we've now copied its data to a PVC
	log.Printf("[%s] Deleting snapshot '%s'\n", jobInput.Namespace, snapshotName)
	err = jobInput.CSIClient.SnapshotV1().VolumeSnapshots(jobInput.Namespace).Delete(
		context.TODO(),
		snapshotName,
		metaV1.DeleteOptions{},
	)
	if err != nil {
		errorChan <- err
		return
	}

	// Create a backup job for the created PVC
	log.Printf("[%s] Creating a backup job for volume '%s'\n", jobInput.Namespace, snapshotPVCName)
	jobName := fmt.Sprintf("backup-%s", snapshotPVCName)
	err = k8s.CreateBackupJob(
		jobInput.ClientSet,
		jobName,
		jobInput.Namespace,
		snapshotPVCName,
	)
	if err != nil {
		errorChan <- err
		return
	}

	log.Printf(
		"[%s] Waiting for backup job '%s' to complete...\n",
		jobInput.Namespace,
		jobName,
	)
	job, err := k8s.WaitForJobReady(jobInput.ClientSet, jobInput.Namespace, jobName)
	if err != nil {
		errorChan <- err
		return
	}

	if job.Status.Failed == 0 {
		log.Printf(
			"[%s] Backup job '%s' to completed successfully\n",
			jobInput.Namespace,
			jobName,
		)

		log.Printf("[%s] Deleting backup job '%s'\n", jobInput.Namespace, jobName)
		err = jobInput.ClientSet.BatchV1().Jobs(jobInput.Namespace).Delete(
			context.TODO(),
			jobName,
			metaV1.DeleteOptions{},
		)
		if err != nil {
			errorChan <- err
			return
		}

		log.Printf("[%s] Deleting pods for backup job '%s'\n", jobInput.Namespace, jobName)
		err = jobInput.ClientSet.CoreV1().Pods(jobInput.Namespace).DeleteCollection(
			context.TODO(),
			metaV1.DeleteOptions{},
			metaV1.ListOptions{
				LabelSelector: fmt.Sprintf("%s=%s", "job-name", jobName),
			},
		)

		log.Printf("[%s] Deleting volume '%s'\n", jobInput.Namespace, snapshotPVCName)
		err = jobInput.ClientSet.CoreV1().PersistentVolumeClaims(jobInput.Namespace).Delete(
			context.TODO(),
			snapshotPVCName,
			metaV1.DeleteOptions{},
		)
		if err != nil {
			errorChan <- err
			return
		}

	} else {
		log.Printf(
			"[%s] One or more pods for backup job '%s' failed\n",
			jobInput.Namespace,
			jobName,
		)
	}
}