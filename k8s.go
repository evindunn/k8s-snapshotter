package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	cephFsAdmin "github.com/ceph/go-ceph/cephfs/admin"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"os"
	"path/filepath"
)

// TODO: Pull fsName (volume name) from storageClass
// TODO: Pull subVolumeGroup from ceph-csi-config configMap

func main() {
	var storageClassName string
	var cephCSINamespace string
	var cephCSIConfigMap string
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
	flag.StringVar(
		&cephCSINamespace,
		"cephCSINamespace",
		"ceph",
		"(optional) The namespace that contains the Ceph CSI ConfigMap",
	)
	flag.StringVar(
		&cephCSIConfigMap,
		"cephCSIConfigMap",
		"ceph-csi-config",
		"(optional) The name of the Ceph CSI ConfigMap",
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

	volumeName, subVolumeGroup, err := getK8sCephMetadata(
		clientSet,
		storageClassName,
		cephCSINamespace,
		cephCSIConfigMap,
	)
	catchError(err)

	cephConnection, err := createCephConnection(cephKeyringUser)
	catchError(err)
	defer cephConnection.Shutdown()

	cephfs := cephFsAdmin.NewFromConn(cephConnection)

	for namespace, pvcCephMaps := range pvcCephMap {
		if len(pvcCephMaps) > 0 {
			fmt.Printf("=== %s ===\n", namespace)
			for _, pvTuple := range pvcCephMaps {
				pvCephPath, err := cephfs.SubVolumePath(
					volumeName,
					subVolumeGroup,
					pvTuple.cephSubVolume,
				)
				catchError(err)

				fmt.Printf("%s -> %s\n", pvTuple.pvcName, pvCephPath)
			}
			fmt.Println()
		}
	}
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

			csiID, err := decomposeCSIID(pv.Spec.CSI.VolumeHandle)
			if err != nil {
				return nil, err
			}

			subVolumeName := fmt.Sprintf("%s%s", volumePrefix, csiID.ObjectID)
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

func getK8sCephMetadata(clientSet *kubernetes.Clientset, storageClassName string, cephCSINamespace string, cephCSIConfigMap string) (string, string, error) {
	var cephConfigJson []CephCSIConfig

	storageClass, err := clientSet.StorageV1().StorageClasses().Get(
		context.TODO(),
		storageClassName,
		metaV1.GetOptions{},
	)
	if err != nil {
		return "", "", err
	}

	cephVolumeName, hasCephVolumeName := storageClass.Parameters["fsName"]
	if !hasCephVolumeName {
		err := fmt.Sprintf(
			"fsName is missing from StorageClass '%s'! Check your Ceph CSI Configuration",
			storageClassName,
		)
		return "", "", errors.New(err)
	}

	cephConfigMap, err := clientSet.CoreV1().ConfigMaps(cephCSINamespace).Get(
		context.TODO(),
		cephCSIConfigMap,
		metaV1.GetOptions{},
	)
	if err != nil {
		return "", "", err
	}

	cephConfigJsonStr, hasCephConfigJson := cephConfigMap.Data["config.json"]
	if !hasCephConfigJson {
		err := fmt.Sprintf(
			"ConfigMap '%s' does not contain the key 'config.json'! Check your Ceph CSI config",
			cephCSIConfigMap,
		)
		return "", "", errors.New(err)
	}

	err = json.Unmarshal([]byte(cephConfigJsonStr), &cephConfigJson)
	if err != nil {
		return "", "", err
	}

	// TODO: Match the correct item to the ceph fsid defined in ceph.conf
	subVolumeGroup := cephConfigJson[0].CephFS.SubvolumeGroup

	// Ceph CSI default
	if subVolumeGroup == "" {
		subVolumeGroup = "csi"
	}

	return cephVolumeName, subVolumeGroup, nil
}

type CephCSIConfig struct {
	ClusterID string 		`json:"clusterID"`
	RadosNamespace string 	`json:"radosNamespace"`
	Monitors []string 		`json:"monitors"`
	CephFS struct {
		SubvolumeGroup string `json:"subvolumeGroup"`
	} `json:"cephFS"`
}

