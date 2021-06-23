package main

// import (
// 	"context"
// 	"flag"
// 	"fmt"
// 	v12 "k8s.io/api/core/v1"
// 	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
// 	"k8s.io/client-go/kubernetes"
// 	"k8s.io/client-go/rest"
// 	"k8s.io/client-go/tools/clientcmd"
// 	"k8s.io/client-go/util/homedir"
// 	"path/filepath"
// 	"strings"
// )
//
// func main() {
// 	isInCluster := flag.Bool(
// 		"inCluster",
// 		false,
// 		"(optional) whether running within a kubernetes cluster",
// 	)
// 	// TODO: Make this required; Right now default is rook-ceph for local testing
// 	cephClusterID := flag.String(
// 		"cephClusterID",
// 		"rook-ceph",
// 		"(optional) the ID of the ceph cluster to back up",
// 	)
// 	flag.Parse()
//
// 	kubeConfig, err := getDefaultKubeConfig(*isInCluster)
// 	catchError(err)
//
// 	clientSet, err := kubernetes.NewForConfig(kubeConfig)
// 	catchError(err)
//
// 	namespaces, err := clientSet.CoreV1().Namespaces().List(context.TODO(), metaV1.ListOptions{})
// 	catchError(err)
//
// 	pvcMap := make(map[string][]v12.PersistentVolumeClaim)
//
// 	// Populate a map of namespaceName --> pvcs
// 	for _, namespace := range namespaces.Items {
// 		pvcs, err := clientSet.CoreV1().PersistentVolumeClaims(namespace.Name).List(
// 			context.TODO(),
// 			metaV1.ListOptions{},
// 		)
// 		catchError(err)
// 		if len(pvcs.Items) > 0 {
// 			pvcMap[namespace.Name] = pvcs.Items
// 		}
// 	}
//
// 	pvMap := make(map[string][]PVTuple)
//
// 	// Populate a map of namespace -> pvcName/cephPath
// 	for namespace, pvcs := range pvcMap {
// 		pvMap[namespace] = make([]PVTuple, 0)
//
// 		for _, pvc := range pvcs {
// 			volumeName := pvc.Spec.VolumeName
//
// 			if volumeName != "" {
// 				pv, err := clientSet.CoreV1().PersistentVolumes().Get(
// 					context.TODO(),
// 					volumeName,
// 					metaV1.GetOptions{},
// 				)
// 				catchError(err)
//
// 				isCSIVolume := pv.Spec.CSI != nil
// 				isCephVolume := strings.HasSuffix(pv.Spec.CSI.Driver, "cephfs.csi.ceph.com")
//
// 				if isCSIVolume && isCephVolume {
// 					volAttrs := pv.Spec.CSI.VolumeAttributes
// 					currentClusterID, hasClusterID := volAttrs["clusterID"]
// 					cephPath, hasCephPath := volAttrs["subvolumePath"]
//
// 					clusterIDMatches := hasClusterID && currentClusterID == *cephClusterID
// 					if clusterIDMatches && hasCephPath {
// 						pvMap[namespace] = append(
// 							pvMap[namespace],
// 							PVTuple{
// 								pvcName: pvc.Name,
// 								cephPath: cephPath,
// 							},
// 						)
// 					}
// 				}
// 			}
// 		}
// 	}
//
// 	for namespace, pvTuples := range pvMap {
// 		fmt.Printf("=== %s ===\n", namespace)
// 		for _, pvTuple := range pvTuples {
// 			fmt.Printf("%s -> %s:%s\n", pvTuple.pvcName, *cephClusterID, pvTuple.cephPath)
// 		}
// 		fmt.Println()
// 	}
// }
//
// /*
// PVTuple - A Mapping between a PersistentVolumeClaim name and it's location on CephFS
// */
// type PVTuple struct {
// 	pvcName string
// 	cephPath string
// }
//
// /*
// getDefaultKubeConfig - If inCluster, returns a k8s Config based on the pod's service account.
// If not, returns a Config based on ~/.kube/config
//  */
// func getDefaultKubeConfig(inCluster bool) (*rest.Config, error) {
// 	if inCluster {
// 		kubeConfig, err := rest.InClusterConfig()
// 		if err != nil {
// 			return &rest.Config{}, err
// 		}
// 		return kubeConfig, nil
// 	}
//
// 	homeDir := homedir.HomeDir()
// 	kubeConfigFile := filepath.Join(
// 		homeDir,
// 		".kube",
// 		"config",
// 	)
// 	kubeConfig, err := clientcmd.BuildConfigFromFlags("", kubeConfigFile)
// 	if err != nil {
// 		return &rest.Config{}, err
// 	}
//
// 	return kubeConfig, nil
// }
