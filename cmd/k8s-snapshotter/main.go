package main

import (
	"context"
	"errors"
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
	var s3BucketUrl string
	var s3BucketName string
	var s3BucketAccessKey string
	var s3BucketSecretKey string
	var isInCluster bool

	flag.StringVar(
		&storageClassName,
		"storageClassName",
		"",
		"The name of the storage class to query volumes from",
	)
	flag.StringVar(
		&s3BucketUrl,
		"s3Url",
		"",
		"The URL to the S3 bucket where backups will be stored",
	)
	flag.StringVar(
		&s3BucketName,
		"s3Bucket",
		"",
		"The name of the S3 bucket where backups will be stored",
	)
	flag.StringVar(
		&s3BucketAccessKey,
		"s3AccessKey",
		"",
		"The access key for the S3 bucket where backups will be stored",
	)
	flag.StringVar(
		&s3BucketSecretKey,
		"s3SecretKey",
		"",
		"The secret key for the S3 bucket where backups will be stored",
	)
	flag.BoolVar(
		&isInCluster,
		"inCluster",
		false,
		"(optional) whether running within a kubernetes cluster. Defaults to false.",
	)

	flag.Parse()

	flagsInvalid := storageClassName == "" ||
		s3BucketUrl == "" ||
		s3BucketName == "" ||
		s3BucketAccessKey == "" ||
		s3BucketSecretKey == ""

	if flagsInvalid {
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

	backupScheduler := utils.NewBackupJobScheduler(
		backupNamespacedPVC,
		clientSet,
		csiClient,
		storageClassName,
		s3BucketUrl,
		s3BucketName,
		s3BucketAccessKey,
		s3BucketSecretKey,
	)

	for _, namespace := range namespaces.Items {
		pvcNames, err := k8s.GetNamespacedPVCs(clientSet, namespace.Name, storageClassName)
		if err != nil {
			utils.CatchError(err)
		}

		if len(pvcNames) > 0 {
			log.Printf("[%s] Starting namespace backup\n", namespace.Name)
		} else {
			log.Printf("[%s] No bound volumes found, skipping namespace\n", namespace.Name)
		}

		for _, pvcName := range pvcNames {
			backupScheduler.Schedule(namespace.Name, pvcName)
		}
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
		log.Fatalln("Backup failed with one or more errors")
	}
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

	// TODO: This needs a shorter signature
	err = k8s.CreateBackupJob(
		jobInput.ClientSet,
		jobName,
		jobInput.S3Url,
		jobInput.S3Bucket,
		jobInput.S3AccessKey,
		jobInput.S3SecretKey,
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
				LabelSelector: fmt.Sprintf("job-name=%s", jobName),
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
		errMsg := fmt.Sprintf(
			"One or more pods for backup job '%s/%s' failed",
			jobInput.Namespace,
			jobName,
		)
		errorChan <- errors.New(errMsg)
	}
}