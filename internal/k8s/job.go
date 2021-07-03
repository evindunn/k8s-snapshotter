package k8s

import (
	"context"
	batchV1 "k8s.io/api/batch/v1"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)


func CreateBackupJob(clientSet *kubernetes.Clientset, jobName string, namespace string, pvcName string) error {
	var job batchV1.Job

	jobLabels := make(map[string]string)
	jobLabels[createdByLabelName] = createdByLabelValue

	jobValues := JobTemplateValues{
		JobName:   jobName,
		JobLabels: jobLabels,
		Namespace: namespace,
		PVCName:   pvcName,
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
