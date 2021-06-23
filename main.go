package main

import (
	"flag"
	"fmt"
	"github.com/ceph/go-ceph/cephfs"
	cephFsAdmin "github.com/ceph/go-ceph/cephfs/admin"
	"log"
	"os"
	"path"
	"time"
)

func main() {
	var clusterUser string
	var volumeName string
	var subVolumeGroup string

	today := time.Now().Format("2006-01-02")
	snapshotNamePrefix := fmt.Sprintf("%s_backup", today)

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
	catchError(err)
	defer connection.Shutdown()

	client := cephFsAdmin.NewFromConn(connection)
	log.Printf("Connected to cluster as user '%s'\n", clusterUser)

	log.Printf("Polling volume '%s' for subvolumes...\n", volumeName)
	subVolumes, err := client.ListSubVolumes(volumeName, subVolumeGroup)
	catchError(err)

	log.Printf("Using snapshot name '%s'\n", snapshotNamePrefix)

	for _, vol := range subVolumes {
		err := client.CreateSubVolumeSnapshot(volumeName, subVolumeGroup, vol, snapshotNamePrefix)
		catchError(err)
	}
	log.Printf("Snapshots created")

	mountInfo, err := cephfs.CreateFromRados(connection)
	catchError(err)
	defer mountInfo.Release()

	err = mountInfo.Mount()
	catchError(err)
	defer mountInfo.Unmount()

	for _, subVolName := range subVolumes {
		subVolumePath, err := client.SubVolumePath(volumeName, subVolumeGroup, subVolName)
		catchError(err)

		snapshots, err := client.ListSubVolumeSnapshots(volumeName, subVolumeGroup, subVolName)
		catchError(err)

		for _, snap := range snapshots {
			// For testing, remove all old snapshots
			if snap != snapshotNamePrefix {
				err := client.RemoveSubVolumeSnapshot(volumeName, subVolumeGroup, subVolName, snap)
				catchError(err)
			}

			snapShotPath := path.Join(subVolumePath, ".snap")
			archiveName := fmt.Sprintf("%s-%s.tar", snapshotNamePrefix, subVolName)

			log.Printf("Creating %s...\n", archiveName)
			err = tarCephDirectory(archiveName, snapShotPath, mountInfo)
			if err != nil {
				catchError(err)
			}
		}
	}

	log.Println("Done")
}