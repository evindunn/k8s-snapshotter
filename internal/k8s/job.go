package k8s

import (
	"context"
	batchV1 "k8s.io/api/batch/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
)

const jobWaitTime = 2

func CreateBackupJob(clientSet *kubernetes.Clientset, jobName string, s3Url string, s3Bucket string, s3AccessKey string, s3SecretKey string, namespace string, pvcName string) error {
	var job batchV1.Job

	jobLabels := make(map[string]string)
	jobLabels[createdByLabelName] = createdByLabelValue

	jobValues := JobTemplateValues{
		JobName:   		jobName,
		JobLabels: 		jobLabels,
		Namespace: 		namespace,
		PVCName:   		pvcName,
		S3Url: 			s3Url,
		S3Bucket: 		s3Bucket,
		S3AccessKey: 	s3AccessKey,
		S3SecretKey: 	s3SecretKey,
	}

	err := ParseK8STemplate(
		JobTemplate,
		"job",
		&jobValues,
		&job,
	)

	if err != nil {
		return err
	}

	_, err = clientSet.BatchV1().Jobs(jobValues.Namespace).Create(
		context.TODO(),
		&job,
		metaV1.CreateOptions{},
	)

	if err != nil {
		return err
	}

	return nil
}

/*
WaitForJobReady waits until the given Job name in the given namespace has one pod with either the
"Completed" or "Failed" status
 */
func WaitForJobReady(clientSet *kubernetes.Clientset, namespace string, jobName string) (*batchV1.Job, error) {
	var job *batchV1.Job
	var err error

	for {
		job, err = clientSet.BatchV1().Jobs(namespace).Get(
			context.TODO(),
			jobName,
			metaV1.GetOptions{},
		)
		if err != nil {
			return nil, err
		}

		// TODO: What if it creates more than one pod?
		if job.Status.Succeeded == 1 || job.Status.Failed == 1 {
			break
		}

		time.Sleep(jobWaitTime * time.Second)
	}

	return job, nil
}