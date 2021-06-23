package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"github.com/ceph/go-ceph/cephfs"
	"github.com/ceph/go-ceph/rados"
	"io"
	"os"
	"path"
	"time"
)

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

// createCephConnection: Returns a new connection to a ceph cluster configured in /etc/ceph/ceph.conf.
// The given user's keyring is used to connect
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

// tarCephDirectory: Tars the the given directory on the given ceph mount and saves the
// file at tarFilePath
func tarCephDirectory(tarFilePath string, directory string, mount *cephfs.MountInfo) error {
	var dirFiles []CephFile

	tarFile, err := os.Create(tarFilePath)
	if err != nil {
		return err
	}
	defer tarFile.Close()

	gzipWriter := gzip.NewWriter(tarFile)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

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

	return nil
}

// walkCephDirectory: Walks the given dirName on the given mount and recursively populates entries
// with the list of CephFiles found
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

// readCephFile: Reads the given path into writer on the given mount
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
