package main

import (
	"context"
	"flag"
	"fmt"
	csiV1 "github.com/kubernetes-csi/external-snapshotter/client/v4/clientset/versioned"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"log"
	"os"
)

// TODO: Pull fsName (volume name) from storageClass
// TODO: Pull subVolumeGroup from ceph-csi-config configMap

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

		// If the namespace doesn't have any, don't include it in the map
		if len(pvcs) > 0 {
			namespacePVCMap[namespace.Name] = pvcs
		}
	}

	csiClient, err := csiV1.NewForConfig(kubeConfig)
	catchError(err)

	for namespace, pvcs := range namespacePVCMap {
		fmt.Printf("===== %s =====\n", namespace)
		for _, pvc := range pvcs {
			snapshot, err := createVolumeSnapshot(csiClient, namespace, pvc)

			if err != nil {
				catchError(err)
			}

			log.Println(snapshot.Name)
		}
		fmt.Println()
	}
}