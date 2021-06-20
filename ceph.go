package main

import (
	"flag"
	"fmt"
	"github.com/ceph/go-ceph/cephfs"
	cephFsAdmin "github.com/ceph/go-ceph/cephfs/admin"
	"github.com/ceph/go-ceph/rados"
	"log"
	"os"
	"path"
)

func main() {
	var clusterUser string
	var volumeName string
	var subVolumeGroup string

	snapshotNamePrefix := "backup"

	flag.StringVar(&clusterUser, "user", "", "The user whose keyring is used to connect to the cluster")
	flag.StringVar(&volumeName, "volume", "", "The volume to poll for subVolumes")
	flag.StringVar(&subVolumeGroup, "subVolumeGroup", "", "The subVolumeGroup to poll for subVolumes")
	flag.Parse()

	if clusterUser == "" || volumeName == "" || subVolumeGroup == "" {
		flag.Usage()
		os.Exit(1)
	}

	log.Printf("Connecting to ceph cluster with user '%s'...\n", clusterUser)
	connection, err := createCephConnection(clusterUser)
	if err != nil {
		panic(err)
	}
	defer connection.Shutdown()

	client := cephFsAdmin.NewFromConn(connection)
	log.Printf("Connected to cluster as user '%s'\n", clusterUser)

	log.Printf("Polling volume '%s' for subvolumes...\n", volumeName)
	subVolumes, err := client.ListSubVolumes(volumeName, subVolumeGroup)
	if err != nil {
		panic(err)
	}

	log.Printf("Using snapshot name '%s'\n", snapshotNamePrefix)
	//
	// for _, vol := range subVolumes {
	// 	err := client.CreateSubVolumeSnapshot(volumeName, subVolumeGroup, vol, snapshotNamePrefix)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// }
	// log.Printf("Snapshots created")

	mountInfo, err := cephfs.CreateFromRados(connection)
	if err != nil {
		panic(err)
	}
	defer mountInfo.Release()

	err = mountInfo.Mount()
	if err != nil {
		panic(err)
	}
	defer mountInfo.Unmount()

	for _, vol := range subVolumes {
		subVolumePath, err := client.SubVolumePath(volumeName, subVolumeGroup, vol)
		if err != nil {
			panic(err)
		}

		snapshots, err := client.ListSubVolumeSnapshots(volumeName, subVolumeGroup, vol)
		if err != nil {
			panic(err)
		}

		for _, snap := range snapshots {
			// For testing, remove all old snapshots
			if snap != snapshotNamePrefix {
				err := client.RemoveSubVolumeSnapshot(volumeName, subVolumeGroup, vol, snap)
				if err != nil {
					panic(err)
				}
			}

			snapShotPath := path.Join(subVolumePath, ".snap")

			var snapShotFiles []string
			err = walkDirectory(&snapShotFiles, mountInfo, snapShotPath)
			if err != nil {
				panic(err)
			}

			for _, file := range snapShotFiles {
				fmt.Println(file)
			}
		}
	}
}

func createCephConnection(clusterUser string) (*rados.Conn, error) {
	connection, err := rados.NewConnWithUser(clusterUser)
	if err != nil {
		return nil, err
	}

	err = connection.ReadDefaultConfigFile()
	if err != nil {
		return nil, err
	}

	err = connection.Connect()
	if err != nil {
		return nil, err
	}

	return connection, nil
}

// func tarDirectory(directory string) error {
// 	// var buffer bytes.Buffer
// 	// tarWriter := tar.NewWriter(&buffer)
//
//
// }

func walkDirectory(entries *[]string, client *cephfs.MountInfo, dirName string) error {
	directory, err := client.OpenDir(dirName)
	if err != nil {
		return err
	}
	defer directory.Close()

	for {
		entry, err := directory.ReadDir()
		if err != nil {
			return err
		}
		if entry == nil {
			break
		}

		entryName := entry.Name()
		if entryName != "." && entryName != ".." {
			entryPath := path.Join(dirName, entryName)

			if entry.DType() == cephfs.DTypeDir {
				err = walkDirectory(entries, client, entryPath)
				if err != nil {
					return err
				}
			} else {
				*entries = append(*entries, entryPath)
			}
		}
	}

	return nil
}
