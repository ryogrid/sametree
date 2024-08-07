package blink_tree

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestNewBufMgr(t *testing.T) {
	type args struct {
		filename string
		bits     uint8
		nodeMax  uint
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "create a new buffer manager",
			args: args{
				filename: "data/buf_mgr_test.db",
				bits:     12,
				nodeMax:  100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.args.filename)
			mgr := NewBufMgr(tt.args.filename, tt.args.bits, tt.args.nodeMax)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}
			page := Page{}
			page_ := &page
			for i := 0; i < 3; i++ {
				if err := mgr.PageIn(page_, Uid(i)); err != BLTErrOk {
					t.Errorf("NewBufMgr() failed to read page. err: %v", err)
				}
			}
			if err := mgr.PageIn(page_, Uid(3)); err != BLTErrRead {
				t.Errorf("NewBufMgr() failed to read page with unexpected err: %v", err)
			}
		})
	}
}

// TODO: test after increment latchDeployed
func TestBufMgr_poolAudit(t *testing.T) {
	type args struct {
		name    string
		bits    uint8
		nodeMax uint
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "pool audit",
			args: args{
				name:    "data/buf_mgr_test.db",
				bits:    12,
				nodeMax: 100,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.args.name)
			mgr := NewBufMgr(tt.args.name, tt.args.bits, tt.args.nodeMax)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}
			mgr.PoolAudit()
		})
	}
}

func TestPageZero_AllocRight(t *testing.T) {
	type fields struct {
		alloc []byte
	}
	tests := []struct {
		name   string
		fields fields
		want   *[BtId]byte
	}{
		{
			name: "get alloc right",
			fields: fields{
				alloc: []byte{
					0, 0, 0, 0, // Cnt
					0, 0, 0, 0, // Act
					0, 0, 0, 0, // Min
					0, 0, 0, 0, // Garbase
					0,                // Bits
					0,                // Free
					0,                // Lvl
					0,                // Kill
					0, 0, 0, 0, 1, 2, // Right
				},
			},
			want: &[BtId]byte{0, 0, 0, 0, 1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := &PageZero{
				alloc: tt.fields.alloc,
			}
			if got := z.AllocRight(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AllocRight() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPageZero_SetAllocRight(t *testing.T) {
	type fields struct {
		alloc []byte
	}
	type args struct {
		pageNo Uid
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []byte
	}{
		{
			name: "get alloc right",
			fields: fields{
				alloc: []byte{
					0, 0, 0, 0, // Cnt
					0, 0, 0, 0, // Act
					0, 0, 0, 0, // Min
					0, 0, 0, 0, // Garbase
					0,                // Bits
					0,                // Free
					0,                // Lvl
					0,                // Kill
					0, 0, 0, 0, 1, 2, // Right
				},
			},
			args: args{
				pageNo: 512,
			},
			want: []byte{0, 0, 0, 0, 2, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			z := &PageZero{
				alloc: tt.fields.alloc,
			}
			z.SetAllocRight(tt.args.pageNo)
			allocLen := len(z.alloc)
			if got := z.alloc[allocLen-BtId : allocLen]; !bytes.Equal(got, tt.want) {
				t.Errorf("SetAllocRight() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBufMgr_PinLatch(t *testing.T) {
	type args struct {
		pageNo Uid
		loadIt bool
		reads  uint
		writes uint
	}
	tests := []struct {
		name        string
		filename    string
		args        args
		wantLatched bool
	}{
		{
			name:     "pin latch",
			filename: "data/pin_latch_test.db",
			args: args{
				pageNo: 3,
				loadIt: false,
				reads:  0,
				writes: 0,
			},
			wantLatched: true,
		},
		{
			name:     "pin latch with loadIt",
			filename: "data/pin_latch_test.db",
			args: args{
				pageNo: 4,
				loadIt: true,
				reads:  0,
				writes: 0,
			},
			wantLatched: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.filename)
			mgr := NewBufMgr(tt.filename, 15, 20)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}
			if tt.args.pageNo > 2 {
				// if pageNo is over 2, we need to write the page to disk
				p := NewPage(mgr.GetPageDataSize())
				mgr.PageOut(p, tt.args.pageNo, true)
			}
			latch := mgr.PinLatch(tt.args.pageNo, tt.args.loadIt, &tt.args.reads, &tt.args.writes)
			if latch == nil && tt.wantLatched {
				t.Errorf("PinLatch() failed to pin latch")
			}

			if latch.pageNo != tt.args.pageNo {
				t.Errorf("PinLatch() failed to set pageNo = %d, want %d", latch.pageNo, tt.args.pageNo)
			}

			if latch.pin != 1 {
				t.Errorf("PinLatch() failed to set pin = %d, want %d", latch.pin, 1)
			}

			if tt.args.loadIt && tt.args.reads != 1 {
				t.Errorf("PinLatch() failed to set reads = %d, want %d", tt.args.reads, 1)
			}
		})
	}
}

func TestBufMgr_PinLatch_Twice(t *testing.T) {
	type args struct {
		pageNo Uid
		reads  uint
		writes uint
	}
	tests := []struct {
		name     string
		filename string
		args     args
	}{
		{
			name:     "pin latch",
			filename: "data/pin_latch_twice_test.db",
			args: args{
				pageNo: 3,
				reads:  0,
				writes: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.filename)
			mgr := NewBufMgr(tt.filename, 15, 20)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}

			_ = mgr.PinLatch(tt.args.pageNo, false, &tt.args.reads, &tt.args.writes)
			latch := mgr.PinLatch(tt.args.pageNo, false, &tt.args.reads, &tt.args.writes)

			if latch.pageNo != tt.args.pageNo {
				t.Errorf("PinLatch() failed to set pageNo = %d, want %d", latch.pageNo, tt.args.pageNo)
			}

			if latch.pin != 2 {
				t.Errorf("PinLatch() failed to set pin = %d, want %d", latch.pin, 2)
			}
		})
	}
}

func TestBufMgr_PinLatch_ClockWise(t *testing.T) {
	type fields struct {
		filename    string
		nodeMax     uint
		unpinPageNo Uid
	}
	type args struct {
		pageNo Uid
		reads  uint
		writes uint
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "pin latch",
			fields: fields{
				filename:    "data/pin_latch_close_wise_test.db",
				nodeMax:     32,
				unpinPageNo: 9,
			},
			args: args{
				pageNo: 34,
				reads:  0,
				writes: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.fields.filename)
			mgr := NewBufMgr(tt.fields.filename, 15, tt.fields.nodeMax)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}

			var unpinLatch *Latchs
			for i := 3; i < int(tt.fields.nodeMax)+2; i++ {
				latch := mgr.PinLatch(Uid(i), false, &tt.args.reads, &tt.args.writes)
				if Uid(i) == tt.fields.unpinPageNo {
					unpinLatch = latch
				}
			}
			if unpinLatch != nil {
				mgr.UnpinLatch(unpinLatch)
			}

			latch := mgr.PinLatch(tt.args.pageNo, false, &tt.args.reads, &tt.args.writes)

			if latch.pageNo != tt.args.pageNo {
				t.Errorf("PinLatch() failed to set pageNo = %d, want %d", latch.pageNo, tt.args.pageNo)
			}

			if latch.pin != 1 {
				t.Errorf("PinLatch() failed to set pin = %d, want %d", latch.pin, 1)
			}
		})
	}
}

func TestBufMgr_UnpinLatch_ClockWise(t *testing.T) {
	type fields struct {
		filename string
		nodeMax  uint
	}
	type args struct {
		reads  uint
		writes uint
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			name: "unpin latch",
			fields: fields{
				filename: "data/unpin_latch_close_wise_test.db",
				nodeMax:  32,
			},
			args: args{
				reads:  0,
				writes: 0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.fields.filename)
			mgr := NewBufMgr(tt.fields.filename, 15, tt.fields.nodeMax)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}

			latch := mgr.PinLatch(2, false, &tt.args.reads, &tt.args.writes)
			if latch.pin != 1 {
				t.Errorf("PinLatch() failed to set pin = %d, want %d", latch.pin, 1)
			}

			mgr.UnpinLatch(latch)
			if latch.pin != 32768 {
				t.Errorf("UnpinLatch() failed to set pin = %d, want %d", latch.pin, 32768)
			}

			FetchAndAndUint32(&latch.pin, ^ClockBit)
			if latch.pin != 0 {
				t.Errorf("FetchAndAndUint32() failed to set pin = %d, want %d", latch.pin, 0)
			}
		})
	}
}

func TestBufMgr_NewPage(t *testing.T) {
	type args struct {
		pageSet PageSet
		page    Page
		reads   uint
		writes  uint
	}
	tests := []struct {
		name     string
		filename string
		args     args
	}{
		{
			name:     "create a new page without reusing empty page",
			filename: "data/new_page_test.db",
			args: args{
				pageSet: PageSet{},
				page:    Page{Data: []byte{1, 2, 3, 4, 5, 6}},
				reads:   0,
				writes:  0,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_ = os.Remove(tt.filename)
			mgr := NewBufMgr(tt.filename, 15, 20)
			if mgr == nil {
				t.Errorf("NewBufMgr() failed")
			}
			initialAllocRight := GetID(mgr.GetPageZero().AllocRight())
			if initialAllocRight != MinLvl+1 {
				t.Errorf("NewBufMgr() failed to initialize allock right")
			}
			if err := mgr.NewPage(&tt.args.pageSet, &tt.args.page, &tt.args.reads, &tt.args.writes); err != BLTErrOk {
				t.Errorf("NewPage() failed to create page with unexpected err: %v", err)
			}

			if got := GetID(mgr.GetPageZero().AllocRight()); got != initialAllocRight+1 {
				t.Errorf("NewPage() failed to increment alloc right = %d, want %d", got, initialAllocRight+1)
			}

			wantData := make([]byte, mgr.GetPageDataSize())
			for i := range tt.args.page.Data {
				wantData[i] = tt.args.page.Data[i]
			}

			if got := tt.args.pageSet.page.Data; !bytes.Equal(got, wantData) {
				t.Errorf("NewPage() failed to map contents = %d, want %d", got, wantData)
			}

			// assert latch data
			// TODO: extract to pinLatch test
			latch := tt.args.pageSet.latch
			if latch == nil {
				t.Errorf("NewPage() failed to set latch")
			}
		})
	}
}
