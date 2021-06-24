package main

import (
	"context"
	"flag"
	"fmt"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
)

func main() {
	var storageClassName string
	var cephKeyringUser string
	var isInCluster bool

	// today := time.Now().Format("2006-01-02")
	// snapshotNamePrefix := fmt.Sprintf("%s_backup", today)

	flag.StringVar(
		&storageClassName,
		"storageClassName",
		"",
		"The name of the storage class to query volumes from",
	)
	flag.StringVar(
		&cephKeyringUser,
		"cephUser",
		"",
		"The name of the Ceph keyring user for connecting to ceph",
	)
	flag.BoolVar(
		&isInCluster,
		"inCluster",
		false,
		"(optional) whether running within a kubernetes cluster",
	)

	flag.Parse()

	if storageClassName == "" || cephKeyringUser == "" {
		flag.Usage()
		os.Exit(1)
	}

	kubeConfig, err := getDefaultKubeConfig(isInCluster)
	catchError(err)

	clientSet, err := kubernetes.NewForConfig(kubeConfig)
	catchError(err)

	namespaces, err := clientSet.CoreV1().Namespaces().List(context.TODO(), metaV1.ListOptions{})
	catchError(err)

	pvcCephMap := make(map[string][]PVCCephMap)

	// Populate a map of namespaceName --> pvcs
	for _, namespace := range namespaces.Items {
		pvcCephMap[namespace.Name], err = getPVCCephMaps(
			clientSet,
			namespace.Name,
			storageClassName,
		)
		catchError(err)
	}

	for namespace, pvcCephMaps := range pvcCephMap {
		if len(pvcCephMaps) > 0 {
			fmt.Printf("=== %s ===\n", namespace)
			for _, pvTuple := range pvcCephMaps {
				fmt.Printf("%s -> %s\n", pvTuple.pvcName, pvTuple.cephSubVolume)
			}
			fmt.Println()
		}
	}

	cephConnection, err := createCephConnection(cephKeyringUser)
	catchError(err)
	defer cephConnection.Shutdown()

	// cephfs := cephFsAdmin.NewFromConn(cephConnection)
	// subVolumes, err := cephfs.ListSubVolumes(volumeName, subVolumeGroup)
}

/*
PVCCephMap - A Mapping between a PersistentVolumeClaim name and it's location on CephFS
*/
type PVCCephMap struct {
	pvcName string
	cephSubVolume string
}

func getPVCCephMaps(clientSet *kubernetes.Clientset, namespace string, storageClassName string) ([]PVCCephMap, error) {
	var pvCephMaps []PVCCephMap

	// Query the volume prefix from the storage class
	storageClass, err := clientSet.StorageV1().StorageClasses().Get(
		context.TODO(),
		storageClassName,
		metaV1.GetOptions{},
	)
	catchError(err)

	volumePrefix, hasVolumePrefix := storageClass.Parameters["volumeNamePrefix"]
	if !hasVolumePrefix {
		// CephFS CSI volume driver default
		volumePrefix = "csi-vol-"
	}

	pvcs, err := clientSet.CoreV1().PersistentVolumeClaims(namespace).List(
		context.TODO(),
		metaV1.ListOptions{},
	)
	if err != nil {
		return nil, err
	}

	for _, pvc := range pvcs.Items {
		isBound := pvc.Status.Phase == "Bound"
		isCorrectStorageClass := *pvc.Spec.StorageClassName == storageClassName

		if isBound && isCorrectStorageClass {
			volumeName := pvc.Spec.VolumeName
			pv, err := clientSet.CoreV1().PersistentVolumes().Get(
				context.TODO(),
				volumeName,
				metaV1.GetOptions{},
			)
			if err != nil {
				return nil, err
			}

			subVolumeName := fmt.Sprintf("%s%s", volumePrefix, pv.UID)
			pvTuple := PVCCephMap{
				pvcName: pvc.Name,
				cephSubVolume: subVolumeName,
			}
			pvCephMaps = append(pvCephMaps, pvTuple)
		}
	}

	return pvCephMaps, nil
}

/*
getDefaultKubeConfig - If inCluster, returns a k8s Config based on the pod's service account.
If not, returns a Config based on ~/.kube/config
 */
func getDefaultKubeConfig(inCluster bool) (*rest.Config, error) {
	if inCluster {
		kubeConfig, err := rest.InClusterConfig()
		if err != nil {
			return &rest.Config{}, err
		}
		return kubeConfig, nil
	}

	homeDir := homedir.HomeDir()
	kubeConfigFile := filepath.Join(
		homeDir,
		".kube",
		"config",
	)
	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigFile)
	if err != nil {
		return &rest.Config{}, err
	}

	return kubeConfig, nil
}
