// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ste

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"time"
	"github.com/Azure/azure-pipeline-go/pipeline"
)

// upload related
const UploadMaxTries = 5
const UploadTryTimeout = time.Minute * 10
const UploadRetryDelay = time.Second * 3
const UploadMaxRetryDelay = time.Second * 12

// download related
const DownloadMaxTries = 5
const DownloadTryTimeout = time.Minute * 10
const DownloadRetryDelay = time.Second * 1
const DownloadMaxRetryDelay = time.Second * 3

// pacer related
const PacerTimeToWaitInMs = 50


//////////////////////////////////////////////////////////////////////////////////////////////////////////

// These types are define the STE Coordinator
type newJobXfer func(jptm IJobPartTransferMgr, pipeline pipeline.Pipeline, pacer *pacer)

// the xfer factory is generated based on the type of source and destination
func computeJobXfer(fromTo common.FromTo) newJobXfer {
	switch fromTo {
	case common.EFromTo.BlobLocal(): // download from Azure Blob to local file system
		return BlobToLocalPrologue
	case common.EFromTo.LocalBlob(): // upload from local file system to Azure blob
		return LocalToBlockBlob
	case common.EFromTo.FileLocal(): // download from Azure File to local file system
		return nil // TODO
	case common.EFromTo.LocalFile(): // upload from local file system to Azure File
		return nil // TODO
	}
	panic(fmt.Errorf("Unrecognized FromTo: %q", fromTo.String()))
}

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
// TODO give these to the plugin packages
type executionEngineHelper struct{}
// opens file with desired flags and return *os.File
func (executionEngineHelper executionEngineHelper) openFile(filePath string, flags int) *os.File {
	f, err := os.OpenFile(filePath, flags, 0644)
	if err != nil {
		panic(fmt.Sprintf("Error opening file: %s", err))
	}
	return f
}

// maps a *os.File into memory and return a byte slice (mmap.MMap)
func (executionEngineHelper executionEngineHelper) mapFile(file *os.File) common.MMF {
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	memoryMappedFile, err := common.NewMMF(file, true, 0, int(fileInfo.Size()))
	if err != nil {
		panic(fmt.Sprintf("Error mapping: %s", err))
	}
	return memoryMappedFile
}

// create and memory map a file, given its path and length
func (executionEngineHelper executionEngineHelper) createAndMemoryMapFile(destinationPath string, fileSize int64) (common.MMF, *os.File) {
	f := executionEngineHelper.openFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		panic(truncateError)
	}

	return executionEngineHelper.mapFile(f), f
}

// open and memory map a file, given its path
func (executionEngineHelper executionEngineHelper) openAndMemoryMapFile(destinationPath string) (common.MMF, *os.File) {
	f := executionEngineHelper.openFile(destinationPath, os.O_RDWR)
	return executionEngineHelper.mapFile(f), f
}
*/