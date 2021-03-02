// Copyright © Microsoft <wastore@microsoft.com>
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
	"sync"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/common"
)

type jobPartCreatedMsg struct {
	totalTransfers       uint32
	isFinalPart          bool
	totalBytesEnumerated uint64
	fileTransfers        uint32
	folderTransfer       uint32
}

type xferDoneMsg = common.TransferDetail
type jobStatusManager struct {
	m           sync.Mutex
	js          *common.ListJobSummaryResponse
}

var jstm jobStatusManager


/*
 * InitStatusMgr is to be performed only when the paused/cancelled job is resumed. 
 * If this routine is called after js is initialized, we'll cause inconsistencies
 * in accounting.
 */
func (jm *jobMgr) InitStatusMgr(js *common.ListJobSummaryResponse) {
	jstm.m.Lock()
	defer jstm.m.Unlock()

	if jstm.js != nil {
		jm.Panic(fmt.Errorf("StatusMgr already init"))
	}

	jstm.js = js
}

/* These functions should not fail */
func (jm *jobMgr) SMUpdateJobpartCreated(msg jobPartCreatedMsg) {
	js := jstm.js
	jstm.m.Lock()
	defer jstm.m.Unlock()

	js.CompleteJobOrdered = js.CompleteJobOrdered || msg.isFinalPart
	js.TotalTransfers += msg.totalTransfers
	js.FileTransfers += msg.fileTransfers
	js.FolderPropertyTransfers += msg.folderTransfer
	js.TotalBytesEnumerated += msg.totalBytesEnumerated
	js.TotalBytesExpected += msg.totalBytesEnumerated
}

func (jm *jobMgr) SMUpdateXferDone(msg xferDoneMsg) {
	js := jstm.js
	jstm.m.Lock()
	defer jstm.m.Unlock()

	switch msg.TransferStatus {
		case common.ETransferStatus.Success():
			js.TransfersCompleted++
			js.TotalBytesTransferred += msg.TransferSize
		case common.ETransferStatus.Failed(),
			common.ETransferStatus.TierAvailabilityCheckFailure(),
			common.ETransferStatus.BlobTierFailure():
			js.TransfersFailed++
			js.FailedTransfers = append(js.FailedTransfers, common.TransferDetail(msg))
		case common.ETransferStatus.SkippedEntityAlreadyExists(),
			common.ETransferStatus.SkippedBlobHasSnapshots():
			js.TransfersSkipped++
			js.SkippedTransfers = append(js.SkippedTransfers, common.TransferDetail(msg))
	}
}

func (jm *jobMgr) ListJobSummary() common.ListJobSummaryResponse {
	jstm.m.Lock()
	defer jstm.m.Unlock()

	js := *jstm.js
	return js
}
