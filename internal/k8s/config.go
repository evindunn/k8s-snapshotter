package k8s

import (
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	"path/filepath"
)

/*
GetDefaultKubeConfig returns a k8s Config based on the pod's service account if inCluster.
If not inCluster, returns a Config based on ~/.kube/config
*/
func GetDefaultKubeConfig(inCluster bool) (*rest.Config, error) {
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