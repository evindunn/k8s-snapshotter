package utils

import (
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"sync"
)

/*
BackupJobInput is the input required to run an asynchronous backup of a given PVCName in a given
Namespace
 */
type BackupJobInput struct {
	ClientSet   *kubernetes.Clientset
	CSIClient   *csiV1.Clientset
	Namespace   string
	PVCName		string
}

/*
BackupJobRunner is the function that is called on each BackupJobInput in order to perform the
backup
 */
type BackupJobRunner func(wg *sync.WaitGroup, output chan <- error, input *BackupJobInput)

/*
BackupJobScheduler is responsible for running BackupJobRunner on multiple BackupJobInput instances in
parallel
 */
type BackupJobScheduler struct {
	jobFunc       BackupJobRunner
	waitGroup     sync.WaitGroup
	inputChannel  chan *BackupJobInput
	outputChannel chan error
}

/*
NewBackupJobScheduler creates a new BackupJobScheduler that will call the given BackupJobRunner on multiple
BackupJobInput instances in parallel
 */
func NewBackupJobScheduler(job BackupJobRunner) *BackupJobScheduler  {
	scheduler := BackupJobScheduler{
		jobFunc:       job,
		waitGroup:     sync.WaitGroup{},
		inputChannel:  make(chan *BackupJobInput),
		outputChannel: make(chan error),
	}
	go scheduler.Run()
	return &scheduler
}

/*
Schedule submits the given BackupJobInput to the scheduler's inputChannel
 */
func (scheduler *BackupJobScheduler) Schedule(input *BackupJobInput)  {
	scheduler.inputChannel <- input
}

/*
Run watches the scheduler's inputChannel, spawning a goroutine running the BackupJobRunner for each
BackupJobInput submitted to the channel
 */
func (scheduler *BackupJobScheduler) Run() {
	defer close(scheduler.outputChannel)
	for input := range scheduler.inputChannel {
		scheduler.waitGroup.Add(1)
		go scheduler.jobFunc(&scheduler.waitGroup, scheduler.outputChannel, input)
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