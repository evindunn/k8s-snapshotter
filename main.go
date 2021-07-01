package main

import (
	"context"
	"flag"
	"fmt"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"os"
)

// TODO: Pull fsName (volume name) from storageClass
// TODO: Pull subVolumeGroup from ceph-csi-config configMap

func main() {
	var storageClassName string
	var isInCluster bool

	// today := time.Now().Format("2006-01-02")
	// snapshotNamePrefix := fmt.Sprintf("%s_backup", today)

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
		"(optional) whether running within a kubernetes cluster",
	)

	flag.Parse()

	if storageClassName == "" {
		flag.Usage()
		os.Exit(1)
	}

	kubeConfig, err := getDefaultKubeConfig(isInCluster)
	catchError(err)

	clientSet, err := kubernetes.NewForConfig(kubeConfig)
	catchError(err)

	namespaces, err := clientSet.CoreV1().Namespaces().List(context.TODO(), metaV1.ListOptions{})
	catchError(err)

	namespacePVCMap := make(map[string][]string)

	for _, namespace := range namespaces.Items {
		pvcs, err := getNamespacedPVCs(clientSet, namespace.Name, storageClassName)
		catchError(err)

		namespacePVCMap[namespace.Name] = pvcs
	}

	for namespace, pvcs := range namespacePVCMap {
		if len(pvcs) > 0 {
			fmt.Printf("===== %s =====\n", namespace)
			for _, pvc := range pvcs {
				fmt.Printf("- %s\n", pvc)
			}
			fmt.Println()
		}
	}
}