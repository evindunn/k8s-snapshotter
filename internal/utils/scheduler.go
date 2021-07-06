package utils

import (
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"sync"
)

/*
BackupJobInput is a namespace/PersistentVolumeClaim name pair that will be backed up
 */
type BackupJobInput struct {
	ClientSet 	*kubernetes.Clientset
	CSIClient	*csiV1.Clientset
	S3Url		string
	S3Bucket	string
	S3AccessKey	string
	S3SecretKey	string
	Namespace 	string
	PVCName		string
}

/*
BackupJobRunner is the function that is called on each BackupJobInput in order to perform the
backup
 */
type BackupJobRunner func(
	wg *sync.WaitGroup,
	output chan <- error,
	input *BackupJobInput,
)

/*
BackupJobScheduler is responsible for running BackupJobRunner on multiple BackupJobInput instances in
parallel
 */
type BackupJobScheduler struct {
	jobFunc       		BackupJobRunner
	waitGroup     		sync.WaitGroup
	inputChannel  		chan *BackupJobInput
	outputChannel 		chan error
	storageClassName	string
	s3Url				string
	s3Bucket			string
	s3AccessKey			string
	s3SecretKey			string
	clientSet   		*kubernetes.Clientset
	csiClient   		*csiV1.Clientset
}

/*
NewBackupJobScheduler creates a new BackupJobScheduler that will call the given BackupJobRunner on multiple
BackupJobInput instances in parallel
 */
func NewBackupJobScheduler(
	job BackupJobRunner,
	clientSet   *kubernetes.Clientset,
	csiClient   *csiV1.Clientset,
	storageClassName string,
	s3Url string,
	s3Bucket string,
	s3AccessKey string,
	s3SecretKey string) *BackupJobScheduler  {

	scheduler := BackupJobScheduler{
		jobFunc:       		job,
		waitGroup:     		sync.WaitGroup{},
		inputChannel:  		make(chan *BackupJobInput),
		outputChannel: 		make(chan error),
		storageClassName: 	storageClassName,
		clientSet: 			clientSet,
		csiClient: 			csiClient,
		s3Url:				s3Url,
		s3Bucket: 			s3Bucket,
		s3AccessKey: 		s3AccessKey,
		s3SecretKey:		s3SecretKey,
	}
	go scheduler.Run()
	return &scheduler
}

/*
Schedule submits the given BackupJobInput to the scheduler's inputChannel
 */
func (scheduler *BackupJobScheduler) Schedule(namespace string, pvcName string)  {
	scheduler.inputChannel <- &BackupJobInput{
		Namespace: namespace,
		PVCName:   pvcName,
	}
}

/*
Run watches the scheduler's inputChannel, spawning a goroutine running the BackupJobRunner for each
BackupJobInput submitted to the channel
 */
func (scheduler *BackupJobScheduler) Run() {
	defer close(scheduler.outputChannel)
	for input := range scheduler.inputChannel {
		scheduler.waitGroup.Add(1)

		jobInput := BackupJobInput{
			ClientSet: scheduler.clientSet,
			CSIClient: scheduler.csiClient,
			S3AccessKey: scheduler.s3AccessKey,
			S3SecretKey: scheduler.s3SecretKey,
			S3Url: scheduler.s3Url,
			S3Bucket: scheduler.s3Bucket,
			Namespace: input.Namespace,
			PVCName:   input.PVCName,
		}
		go scheduler.jobFunc(
			&scheduler.waitGroup,
			scheduler.outputChannel,
			&jobInput,
		)
	}
	scheduler.waitGroup.Wait()
}

/*
Wait closes the scheduler's inputChannel. This triggers the end of the loop in Run, which in turn
closes the scheduler's outputChannel. The outputChannel is then returned so it can be read from.
 */
func (scheduler *BackupJobScheduler) Wait() chan error {
	close(scheduler.inputChannel)
	return scheduler.outputChannel
}