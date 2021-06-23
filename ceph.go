package main

import (
	"archive/tar"
	"bufio"
	"flag"
	"fmt"
	"github.com/ceph/go-ceph/cephfs"
	cephFsAdmin "github.com/ceph/go-ceph/cephfs/admin"
	"github.com/ceph/go-ceph/rados"
	"io"
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

func tarCephDirectory(tarFilePath string, directory string, mount *cephfs.MountInfo) error {
	var dirFiles []CephFile

	tarFile, err := os.Create(tarFilePath)
	if err != nil {
		return err
	}
	tarWriter := tar.NewWriter(tarFile)
	tarBuffer := bufio.NewWriter(tarWriter)

	err = mount.ChangeDir(directory)
	if err != nil {
		return err
	}

	err = walkCephDirectory(&dirFiles, mount, ".")
	if err != nil {
		return err
	}

	for _, cephFile := range dirFiles {
		fileHeader := &tar.Header{
			Name: cephFile.path,
			Uid: cephFile.uid,
			Gid: cephFile.gid,
			Mode: cephFile.mode,
			AccessTime: cephFile.accessed,
			ModTime: cephFile.modified,
			Size: cephFile.size,
		}

		err = tarWriter.WriteHeader(fileHeader)
		if err != nil {
			return err
		}

		err = readCephFile(tarBuffer, mount, cephFile.path)
		if err != nil {
			return err
		}

		err = tarBuffer.Flush()
		if err != nil {
			return err
		}
	}

	err = tarWriter.Close()
	if err != nil {
		return err
	}

	return nil
}

func walkCephDirectory(entries *[]CephFile, mount *cephfs.MountInfo, dirName string) error {
	directory, err := mount.OpenDir(dirName)
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
				err = walkCephDirectory(entries, mount, entryPath)
				if err != nil {
					return err
				}
			} else {
				// Third arg isn't used, it's only when a new file is created
				file, err := mount.Open(entryPath, os.O_RDONLY, 0644)
				if err != nil {
					return err
				}

				stats, err := file.Fstatx(cephfs.StatxBasicStats, 0)
				if err != nil {
					return err
				}

				err = file.Close()
				if err != nil {
					return err
				}

				cephFile := CephFile{
					uid:  int(stats.Uid),
					gid:  int(stats.Gid),
					mode: int64(stats.Mode),
					accessed: time.Unix(stats.Atime.Sec, stats.Atime.Nsec),
					modified: time.Unix(stats.Mtime.Sec, stats.Mtime.Nsec),
					size: int64(stats.Size),
					path: entryPath,
				}
				*entries = append(*entries, cephFile)
			}
		}
	}

	return nil
}

func readCephFile(writer *bufio.Writer, mount *cephfs.MountInfo, path string) error {
	// Third arg isn't used, it's only when a new file is created
	file, err := mount.Open(path, os.O_RDONLY, 0644)
	defer file.Close()

	if err != nil {
		return err
	}

	fileBuffer := bufio.NewReader(file)
	_, err = io.Copy(writer, fileBuffer)

	if err != nil {
		return err
	}

	return nil
}

// CephFile - Struct representing a CephFS file that will eventually be tar'd
type CephFile struct {
	uid int
	gid int
	mode int64
	accessed time.Time
	modified time.Time
	size int64
	path string
}