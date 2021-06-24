package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/ceph/go-ceph/cephfs"
	"github.com/ceph/go-ceph/rados"
	"io"
	"os"
	"path"
	"time"
)

// CephDirEnt - Struct representing a CephFS file/directory/symlink that will eventually be tar'd
type CephDirEnt struct {
	uid int
	gid int
	mode int64
	accessed time.Time
	modified time.Time
	size int64
	path string
	isDir bool
	linkTarget string
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
	var dirEntries []CephDirEnt

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

	err = walkCephDirectory(&dirEntries, mount, ".")
	if err != nil {
		return err
	}

	for _, cephDirEnt := range dirEntries {
		entryHeader := &tar.Header{
			Name:       cephDirEnt.path,
			Uid:        cephDirEnt.uid,
			Gid:        cephDirEnt.gid,
			Mode:       cephDirEnt.mode,
			AccessTime: cephDirEnt.accessed,
			ModTime:    cephDirEnt.modified,
			Size:       cephDirEnt.size,
		}

		if cephDirEnt.isDir {
			entryHeader.Typeflag = byte(tar.TypeDir)
		} else if cephDirEnt.linkTarget != "" {
			entryHeader.Typeflag = byte(tar.TypeLink)
			entryHeader.Linkname = cephDirEnt.linkTarget
		} else {
			entryHeader.Typeflag = byte(tar.TypeReg)
		}

		err = tarWriter.WriteHeader(entryHeader)
		if err != nil {
			return err
		}

		if entryHeader.Typeflag == tar.TypeReg {
			err = readCephFile(tarBuffer, mount, cephDirEnt.path)
			if err != nil {
				return err
			}
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
func walkCephDirectory(entries *[]CephDirEnt, mount *cephfs.MountInfo, dirName string) error {
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
			entryStats, err := mount.Statx(entryPath, cephfs.StatxBasicStats, 0)
			if err != nil {
				return err
			}

			cephDirEnt := CephDirEnt{
				uid:        int(entryStats.Uid),
				gid:        int(entryStats.Gid),
				mode:       int64(entryStats.Mode),
				accessed:   time.Unix(entryStats.Atime.Sec, entryStats.Atime.Nsec),
				modified:   time.Unix(entryStats.Mtime.Sec, entryStats.Mtime.Nsec),
				size:       int64(entryStats.Size),
				path:       entryPath,
				isDir:      entry.DType() == cephfs.DTypeDir,
				linkTarget: "",
			}

			if entry.DType() == cephfs.DTypeLnk {
				target, err := mount.Readlink(entryPath)
				if err != nil {
					return err
				}
				cephDirEnt.linkTarget = target
			}

			// Append to list
			*entries = append(*entries, cephDirEnt)

			// Recurse if directory
			if entry.DType() == cephfs.DTypeDir {
				err = walkCephDirectory(entries, mount, entryPath)
				if err != nil {
					return err
				}
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

// decomposeCSIID uses the scheme defined in https://github.com/ceph/ceph-csi/blob/v3.3.1/internal/util/volid.go
// to parse a Ceph CSI Volume ID
func decomposeCSIID(csiID string) (*CephFSCSIID, error) {
	const VersionBytes = 4
	const ClusterIdLenBytes = 4
	const MaxClusterIDLen = 36
	const PoolIdBytes = 16
	const UuidSize = 36

	var id CephFSCSIID
	currentIdx := 0

	encodingVersionStr, err := hex.DecodeString(csiID[currentIdx:VersionBytes])
	if err != nil {
		return nil, err
	}
	id.EncodingVersion = int(binary.BigEndian.Uint16(encodingVersionStr))

	if id.EncodingVersion != 1 {
		return nil, errors.New(fmt.Sprintf("unsupported Ceph Volume ID (version %d)", id.EncodingVersion))
	}

	// Skip 1 byte for the '-' separator
	currentIdx += VersionBytes + 1

	clusterIDLenStr, err := hex.DecodeString(csiID[currentIdx:currentIdx + ClusterIdLenBytes])
	if err != nil {
		return nil, err
	}
	clusterIDLen := int(binary.BigEndian.Uint16(clusterIDLenStr))

	if clusterIDLen > MaxClusterIDLen {
		return nil, errors.New("invalid ClusterID")
	}

	// Skip 1 byte for the '-' separator
	currentIdx += ClusterIdLenBytes + 1

	id.ClusterID = csiID[currentIdx:currentIdx + clusterIDLen]

	// Skip 1 byte for the '-' separator
	currentIdx += clusterIDLen + 1

	poolIdStr, err := hex.DecodeString(csiID[currentIdx:currentIdx + PoolIdBytes])
	if err != nil {
		return nil, err
	}

	id.PoolID = int64(binary.BigEndian.Uint64(poolIdStr))

	// Skip 1 byte for the '-' separator
	currentIdx += PoolIdBytes + 1

	id.ObjectID = csiID[currentIdx:]

	if len(id.ObjectID) != UuidSize {
		return nil, errors.New("invalid ObjectID")
	}

	return &id, nil
}

type CephFSCSIID struct {
	EncodingVersion int
	ClusterID string
	PoolID int64
	ObjectID string
}
