package utils

import (
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	"k8s.io/client-go/kubernetes"
	"sync"
)

type BackupJobInput struct {
	ClientSet   *kubernetes.Clientset
	CSIClient   *csiV1.Clientset
	Namespace   string
	PVCName		string
}

type BackupJobRunner func(wg *sync.WaitGroup, output chan <- error, input *BackupJobInput)

type BackupJobScheduler struct {
	jobFunc       BackupJobRunner
	waitGroup     sync.WaitGroup
	inputChannel  chan *BackupJobInput
	outputChannel chan error
}

func NewBackupJobScheduler(job BackupJobRunner) BackupJobScheduler  {
	return BackupJobScheduler{
		jobFunc:       job,
		waitGroup:     sync.WaitGroup{},
		inputChannel:  make(chan *BackupJobInput),
		outputChannel: make(chan error),
	}
}

func (scheduler *BackupJobScheduler) Schedule(input *BackupJobInput)  {
	scheduler.inputChannel <- input
}

func (scheduler *BackupJobScheduler) Run() {
	defer close(scheduler.outputChannel)
	for input := range scheduler.inputChannel {
		scheduler.waitGroup.Add(1)
		go scheduler.jobFunc(&scheduler.waitGroup, scheduler.outputChannel, input)
	}
	scheduler.waitGroup.Wait()
}

func (scheduler *BackupJobScheduler) Wait() chan error {
	close(scheduler.inputChannel)
	return scheduler.outputChannel
}