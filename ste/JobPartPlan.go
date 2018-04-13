package ste

import (
	"errors"
	"github.com/Azure/azure-storage-azcopy/common"
	"reflect"
	"sync/atomic"
	"unsafe"
)

// dataSchemaVersion defines the data schema version of JobPart order files supported by
// current version of azcopy
// To be Incremented every time when we release azcopy with changed dataSchema
const DataSchemaVersion common.Version = 0

const (
	ContentTypeMaxBytes     = 256  // If > 65536, then jobPartPlanBlobData's ContentTypeLength's type  field must change
	ContentEncodingMaxBytes = 256  // If > 65536, then jobPartPlanBlobData's ContentEncodingLength's type  field must change
	MetadataMaxBytes        = 1000 // If > 65536, then jobPartPlanBlobData's MetadataLength field's type must change
	BlobTierMaxBytes        = 10
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type JobPartPlanMMF common.MMF

func (mmf JobPartPlanMMF) Plan() *JobPartPlanHeader {
	// getJobPartPlanPointer returns the memory map JobPartPlanHeader pointer
	// casting the mmf slice's address  to JobPartPlanHeader Pointer
	return (*JobPartPlanHeader)(unsafe.Pointer((*reflect.SliceHeader)(unsafe.Pointer(&mmf)).Data))
}
func (mmf *JobPartPlanMMF) Unmap() { (*common.MMF)(mmf).Unmap() }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// JobPartPlanHeader represents the header of Job Part's memory-mapped file
type JobPartPlanHeader struct {
	// Once set, the following fields are constants; they should never be modified
	Version            common.Version               // The version of data schema format of header; see the dataSchemaVersion constant
	JobID              common.JobID        // Job Part's JobID
	PartNum            common.PartNumber   // Job Part's part number (0+)
	IsFinalPart        bool                // True if this is the Job's last part; else false
	Priority           common.JobPriority  // The Job Part's priority
	TTLAfterCompletion uint32              // Time to live after completion is used to persists the file on disk of specified time after the completion of JobPartOrder
	FromTo             common.FromTo       // The location of the transfer's source & destination
	NumTransfers       uint32              // The number of transfers in the Job part
	LogLevel           common.LogLevel     // This Job Part's minimal log level
	DstBlobData        JobPartPlanDstBlob  // Additional data for blob destinations
	DstLocalData       JobPartPlanDstLocal // Additional data for local destinations

	// Any fields below this comment are NOT constants; they may change over as the job part is processed.
	// Care must be taken to read/write to these fields in a thread-safe way!

	// jobStatus_doNotUse represents the current status of JobPartPlan
	// jobStatus_doNotUse is a private member whose value can be accessed by Status and SetJobStatus
	// jobStatus_doNotUse should not be directly accessed anywhere except by the Status and SetJobStatus
	atomicJobStatus common.JobStatus
}

// Status returns the job status stored in JobPartPlanHeader in thread-safe manner
func (jpph *JobPartPlanHeader) JobStatus() common.JobStatus {
	return common.JobStatus{Value: atomic.LoadUint32(&jpph.atomicJobStatus.Value)}
}

// SetJobStatus sets the job status in JobPartPlanHeader in thread-safe manner
func (jpph *JobPartPlanHeader) SetJobStatus(status common.JobStatus) {
	atomic.StoreUint32(&jpph.atomicJobStatus.Value, status.Value)
}

// Transfer api gives memory map JobPartPlanTransfer header for given index
func (jpph *JobPartPlanHeader) Transfer(transferIndex uint32) *JobPartPlanTransfer {
	// get memory map JobPartPlan Header Pointer
	if transferIndex >= jpph.NumTransfers {
		panic(errors.New("requesting a transfer index greater than what is available"))
	}

	// (Job Part Plan's file address) + (header size) --> beginning of transfers in file
	// Add (transfer size) * (transfer index)
	return (*JobPartPlanTransfer)(unsafe.Pointer((uintptr(unsafe.Pointer(jpph)) + unsafe.Sizeof(*jpph)) + (unsafe.Sizeof(JobPartPlanTransfer{}) * uintptr(transferIndex))))
}

// getTransferSrcDstDetail return the source and destination string for a transfer at given transferIndex in JobPartOrder
func (jpph *JobPartPlanHeader) TransferSrcDstStrings(transferIndex uint32) (source, destination string) {
	jppt := jpph.Transfer(transferIndex)

	srcSlice := []byte{}
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&srcSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(jppt.SrcOffset) // Address of Job Part Plan + this transfer's src string offset
	sh.Len = int(jppt.SrcLength)
	sh.Cap = sh.Len

	dstSlice := []byte{}
	sh = (*reflect.SliceHeader)(unsafe.Pointer(&dstSlice))
	sh.Data = uintptr(unsafe.Pointer(jpph)) + uintptr(jppt.SrcOffset) + uintptr(jppt.SrcLength) // Address of Job Part Plan + this transfer's src string offset + length of this transfer's src string
	sh.Len = int(jppt.DstLength)
	sh.Cap = sh.Len

	return string(srcSlice), string(dstSlice)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// JobPartPlanDstBlob holds additional settings required when the destination is a blob
type JobPartPlanDstBlob struct {
	// Once set, the following fields are constants; they should never be modified

	// represents user decision to interpret the content-encoding from source file
	NoGuessMimeType bool

	// Specifies the length of MIME content type of the blob
	ContentTypeLength uint16

	// Specifies the MIME content type of the blob. The default type is application/octet-stream
	ContentType [ContentTypeMaxBytes]byte

	// Specifies length of content encoding which have been applied to the blob.
	ContentEncodingLength uint16

	// Specifies which content encodings have been applied to the blob.
	ContentEncoding [ContentEncodingMaxBytes]byte

	// Specifies the length of BlockBlobTier of the blob.
	BlockBlobTierLength uint8

	// Specifies the tier on the block blob.
	BlockBlobTier  [BlobTierMaxBytes]byte

	// Specifies the length of PageBlobTier of the blob.
	PageBlobTierLength uint8

	// Specifies the tier on the page blob.
	PageBlobTier  [BlobTierMaxBytes]byte

	MetadataLength uint16
	Metadata       [MetadataMaxBytes]byte

	// Specifies the maximum size of block which determines the number of chunks and chunk size of a transfer
	BlockSize uint32
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// jobPartPlanDstLocal holds additional settings required when the destination is a local file
type JobPartPlanDstLocal struct {
	// Once set, the following fields are constants; they should never be modified

	// Specifies whether the timestamp of destination file has to be set to the modified time of source file
	PreserveLastModifiedTime bool
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// JobPartPlanTransfer represent the header of Job Part's Transfer in Memory Map File
type JobPartPlanTransfer struct {
	// Once set, the following fields are constants; they should never be modified

	// SrcOffset represents the actual start offset transfer header written in JobPartOrder file
	SrcOffset int64
	// SrcLength represents the actual length of source string for specific transfer
	SrcLength int16
	// DstLength represents the actual length of destination string for specific transfer
	DstLength int16
	// ChunkCount represents the num of chunks a transfer is split into
	//ChunkCount uint16	// TODO: Remove this, we need to determine it at runtime
	// ModifiedTime represents the last time at which source was modified before start of transfer stored as nanoseconds.
	ModifiedTime int64
	// SourceSize represents the actual size of the source on disk
	SourceSize int64
	// CompletionTime represents the time at which transfer was completed
	CompletionTime uint64

	// Any fields below this comment are NOT constants; they may change over as the transfer is processed.
	// Care must be taken to read/write to these fields in a thread-safe way!

	// transferStatus_doNotUse represents the status of current transfer (TransferInProgress, TransferFailed or TransfersCompleted)
	// transferStatus_doNotUse should not be directly accessed anywhere except by transferStatus and setTransferStatus
	atomicTransferStatus common.TransferStatus
}

// TransferStatus returns the transfer's status
func (jppt *JobPartPlanTransfer) TransferStatus() common.TransferStatus {
	return common.TransferStatus{Value: atomic.LoadInt32(&jppt.atomicTransferStatus.Value)}
}

// SetTransferStatus sets the transfer's status
func (jppt *JobPartPlanTransfer) SetTransferStatus(status common.TransferStatus) {
	common.AtomicMorphInt32(&jppt.atomicTransferStatus.Value,
		func(startVal int32) (val int32, morphResult interface{}) {
			return common.Iffint32(startVal == (common.TransferStatus{}).Failed().Value, startVal, status.Value), nil
		})
}