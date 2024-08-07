package blink_tree

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync/atomic"
)

type (
	PageZero struct {
		alloc []byte      // next page_no in right ptr
		dups  uint64      // global duplicate key unique id
		chain [BtId]uint8 // head of free page_nos chain
	}
	BufMgrOrgImpl struct {
		pageSize     uint32 // page size
		pageBits     uint8  // page size in bits
		pageDataSize uint32 // page data size
		idx          *os.File

		pageZero      PageZero
		lock          SpinLatch   // allocation area lite latch
		latchDeployed uint32      // highest number of latch entries deployed
		nLatchPage    uint        // number of latch pages at BT_latch
		latchTotal    uint        // number of page latch entries
		latchHash     uint        // number of latch hash table slots (latch hash table slots の数)
		latchVictim   uint32      // next latch entry to examine
		hashTable     []HashEntry // the buffer pool hash table entries
		latchSets     []Latchs    // mapped latch set from buffer pool
		pagePool      []Page      // mapped to the buffer pool pages

		err BLTErr // last error
	}
)

func (z *PageZero) AllocRight() *[BtId]byte {
	rightStart := 4*4 + 1 + 1 + 1 + 1
	return (*[6]byte)(z.alloc[rightStart : rightStart+6])
}

func (z *PageZero) SetAllocRight(pageNo Uid) {
	PutID(z.AllocRight(), pageNo)
}

// NewBufMgr creates a new buffer manager
func NewBufMgr(name string, bits uint8, nodeMax uint) BufMgr {
	initit := true

	// determine sanity of page size
	if bits > BtMaxBits {
		bits = BtMaxBits
	} else if bits < BtMinBits {
		bits = BtMinBits
	}

	// determine sanity of buffer pool
	if nodeMax < HASH_TABLE_ENTRY_CHAIN_LEN {
		errPrintf("Buffer pool too small: %d\n", nodeMax)
		return nil
	}

	var err error

	mgr := BufMgrOrgImpl{}
	mgr.idx, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		errPrintf("Unable to open btree file: %v\n", err)
		return nil
	}

	// data to map to BufMgrOrgImpl::pageZero.alloc
	var pageZeroBytes []byte

	// read minimum page size to get root info
	//  to support raw disk partition files
	//  check if bits == 0 on the disk.
	if size, err := mgr.idx.Seek(0, io.SeekEnd); size > 0 && err == nil {
		pageBytes := make([]byte, BtMinPage)

		if n, err := mgr.idx.ReadAt(pageBytes, 0); err == nil && n == BtMinPage {
			var page Page

			if err := binary.Read(bytes.NewReader(pageBytes), binary.LittleEndian, &page.PageHeader); err != nil {
				errPrintf("Unable to read btree file: %v\n", err)
				return nil
			}
			page.Data = pageBytes[PageHeaderSize:]
			pageZeroBytes = pageBytes

			if page.Bits > 0 {
				bits = page.Bits
				initit = false
			}
		}
	}

	mgr.pageSize = 1 << bits
	mgr.pageBits = bits
	mgr.pageDataSize = mgr.pageSize - PageHeaderSize

	// calculate number of latch hash table entries
	// Note: in original code, calculate using HashEntry size
	// `mgr->nlatchpage = (nodemax/HASH_TABLE_ENTRY_CHAIN_LEN * sizeof(HashEntry) + mgr->page_size - 1) / mgr->page_size;`
	mgr.latchHash = nodeMax / HASH_TABLE_ENTRY_CHAIN_LEN

	mgr.latchTotal = nodeMax

	if initit {
		alloc := NewPage(mgr.pageDataSize)
		alloc.Bits = mgr.pageBits
		PutID(&alloc.Right, MinLvl+1)

		if mgr.PageOut(alloc, 0, true) != BLTErrOk {
			errPrintf("Unable to create btree page zero\n")
			mgr.Close()
			return nil
		}

		// store page zero data to map to BufMgrOrgImpl::pageZero.alloc
		buf := bytes.NewBuffer(make([]byte, 0, mgr.pageSize))
		if err2 := binary.Write(buf, binary.LittleEndian, alloc.PageHeader); err2 != nil {
			errPrintf("Unable to output page header as bytes: %v\n", err2)
		}
		pageZeroBytes = buf.Bytes()

		alloc = NewPage(mgr.pageDataSize)
		alloc.Bits = mgr.pageBits

		for lvl := MinLvl - 1; lvl >= 0; lvl-- {
			z := uint32(1) // size of BLTVal
			if lvl > 0 {   // only page 0
				z += BtId
			}
			alloc.SetKeyOffset(1, mgr.pageDataSize-3-z)
			// create stopper key
			alloc.SetKey([]byte{0xff, 0xff}, 1)

			if lvl > 0 {
				var value [BtId]byte
				PutID(&value, Uid(MinLvl-lvl+1))
				alloc.SetValue(value[:], 1)
			} else {
				alloc.SetValue([]byte{}, 1)
			}

			alloc.Min = alloc.KeyOffset(1)
			alloc.Lvl = uint8(lvl)
			alloc.Cnt = 1
			alloc.Act = 1

			if err := mgr.PageOut(alloc, Uid(MinLvl-lvl), true); err != BLTErrOk {
				errPrintf("Unable to create btree page zero\n")
				return nil
			}
		}

	}

	//flag := syscall.PROT_READ | syscall.PROT_WRITE
	//mgr.pageZero.alloc, err = syscall.Mmap(int(mgr.idx.Fd()), 0, int(mgr.pageSize), flag, syscall.MAP_SHARED)
	//if err != nil {
	//	errPrintf("Unable to mmap btree page zero: %v\n", err)
	//	mgr.Close()
	//	return nil
	//}
	mgr.pageZero.alloc = pageZeroBytes

	// comment out because of panic
	//if err := syscall.Mlock(mgr.pageZero); err != nil {
	//	log.Panicf("Unable to mlock btree page zero: %v", err)
	//}

	mgr.hashTable = make([]HashEntry, mgr.latchHash)
	mgr.latchSets = make([]Latchs, mgr.latchTotal)
	mgr.pagePool = make([]Page, mgr.latchTotal)

	return &mgr
}

func (mgr *BufMgrOrgImpl) PageIn(page *Page, pageNo Uid) BLTErr {
	off := pageNo << mgr.pageBits

	//fmt.Println("PageIn pageNo: ", pageNo, " off: ", off)

	pageBytes := make([]byte, mgr.pageSize)
	if n, err := mgr.idx.ReadAt(pageBytes, int64(off)); err != nil || n < int(mgr.pageSize) {
		errPrintf("Unable to read page. Because of err: %v or n: %d\n", err, n)
		return BLTErrRead
	}

	if err := binary.Read(bytes.NewReader(pageBytes), binary.LittleEndian, &page.PageHeader); err != nil {
		errPrintf("Unable to read page header as bytes: %v\n", err)
		return BLTErrRead
	}
	page.Data = pageBytes[PageHeaderSize:]

	return BLTErrOk
}

// writePage writes a page to permanent location in BLTree file,
// and clear the dirty bit (← clear していない...)
func (mgr *BufMgrOrgImpl) PageOut(page *Page, pageNo Uid, isDirty bool) BLTErr {
	//fmt.Println("PageOut pageNo: ", pageNo)

	off := pageNo << mgr.pageBits
	// write page to disk as []byte
	buf := bytes.NewBuffer(make([]byte, 0, mgr.pageSize))
	if err := binary.Write(buf, binary.LittleEndian, page.PageHeader); err != nil {
		errPrintf("Unable to output page header as bytes: %v\n", err)
		return BLTErrWrite
	}
	if _, err := buf.Write(page.Data); err != nil {
		errPrintf("Unable to output page data: %v\n", err)
		return BLTErrWrite
	}
	if buf.Len() < int(mgr.pageSize) {
		buf.Write(make([]byte, int(mgr.pageSize)-buf.Len()))
	}
	if _, err := mgr.idx.WriteAt(buf.Bytes(), int64(off)); err != nil {
		errPrintf("Unable to write btree file: %v\n", err)
		return BLTErrWrite
	}

	return BLTErrOk
}

// Close
//
// flush dirty pool pages to the btree and close the btree file
func (mgr *BufMgrOrgImpl) Close() {
	num := 0

	// flush page 0
	pageZero := NewPage(mgr.pageDataSize)
	pageZero.PageHeader.Right = *mgr.pageZero.AllocRight()
	pageZero.PageHeader.Bits = mgr.pageBits
	mgr.PageOut(pageZero, 0, true)

	// flush dirty pool pages to the btree
	var slot uint32
	for slot = 1; slot <= mgr.latchDeployed; slot++ {
		page := &mgr.pagePool[slot]
		latch := &mgr.latchSets[slot]

		if latch.dirty {
			mgr.PageOut(page, latch.pageNo, true)
			latch.dirty = false
			num++
		}
	}

	errPrintf("%d buffer pool pages flushed\n", num)

	//if err := syscall.Munmap(mgr.pageZero.alloc); err != nil {
	//	errPrintf("Unable to munmap btree page zero: %v\n", err)
	//}

	if err := mgr.idx.Close(); err != nil {
		errPrintf("Unable to close btree file: %v\n", err)
	}
}

// poolAudit
func (mgr *BufMgrOrgImpl) PoolAudit() {
	var slot uint32
	for slot = 0; slot <= mgr.latchDeployed; slot++ {
		latch := mgr.latchSets[slot]

		if (latch.readWr.rin & Mask) > 0 {
			errPrintf("latchset %d rwlocked for page %d\n", slot, latch.pageNo)
		}
		latch.readWr = BLTRWLock{}

		if (latch.access.rin & Mask) > 0 {
			errPrintf("latchset %d access locked for page %d\n", slot, latch.pageNo)
		}
		latch.access = BLTRWLock{}

		if (latch.parent.rin & Mask) > 0 {
			errPrintf("latchset %d parentlocked for page %d\n", slot, latch.pageNo)
		}
		latch.parent = BLTRWLock{}

		if (latch.pin & ^ClockBit) > 0 {
			errPrintf("latchset %d pinned for page %d\n", slot, latch.pageNo)
			latch.pin = 0
		}
	}
}

// latchLink
func (mgr *BufMgrOrgImpl) LatchLink(hashIdx uint, slot uint, pageNo Uid, loadIt bool, reads *uint) BLTErr {
	page := &mgr.pagePool[slot]
	latch := &mgr.latchSets[slot]

	if he := &mgr.hashTable[hashIdx]; he != nil {
		latch.next = he.slot
		if he.slot > 0 {
			mgr.latchSets[latch.next].prev = slot
		}
	} else {
		panic("hash table entry is nil")
	}

	mgr.hashTable[hashIdx].slot = slot
	latch.atomicID = 0
	latch.pageNo = pageNo
	latch.entry = slot
	latch.split = 0
	latch.prev = 0
	latch.pin = 1

	if loadIt {
		if mgr.err = mgr.PageIn(page, pageNo); mgr.err != BLTErrOk {
			return mgr.err
		}
		*reads++
	}

	mgr.err = BLTErrOk
	return mgr.err
}

// MapPage maps a page from the buffer pool
func (mgr *BufMgrOrgImpl) GetRefOfPageAtPool(latch *Latchs) *Page {
	return &mgr.pagePool[latch.entry]
}

// PinLatch pins a page in the buffer pool
func (mgr *BufMgrOrgImpl) PinLatch(pageNo Uid, loadIt bool, reads *uint, writes *uint) *Latchs {
	hashIdx := uint(pageNo) % mgr.latchHash

	// try to find our entry
	mgr.hashTable[hashIdx].latch.SpinWriteLock()
	defer mgr.hashTable[hashIdx].latch.SpinReleaseWrite()

	slot := mgr.hashTable[hashIdx].slot
	for slot > 0 {
		latch := &mgr.latchSets[slot]
		if latch.pageNo == pageNo {
			break
		}
		slot = latch.next
	}

	// found our entry increment clock
	if slot > 0 {
		latch := &mgr.latchSets[slot]
		atomic.AddUint32(&latch.pin, 1)

		return latch
	}

	// see if there are any unused pool entries

	slot = uint(atomic.AddUint32(&mgr.latchDeployed, 1))
	if slot < mgr.latchTotal {
		latch := &mgr.latchSets[slot]
		if mgr.LatchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			return nil
		}

		return latch
	}

	atomic.AddUint32(&mgr.latchDeployed, DECREMENT)

	for {
		slot = uint(atomic.AddUint32(&mgr.latchVictim, 1) - 1)

		// try to get write lock on hash chain
		// skip entry if not obtained or has outstanding pins
		slot %= mgr.latchTotal

		if slot == 0 {
			continue
		}
		latch := &mgr.latchSets[slot]
		idx := uint(latch.pageNo) % mgr.latchHash

		// see we are on same chain as hashIdx
		if idx == hashIdx {
			continue
		}
		if !mgr.hashTable[idx].latch.SpinWriteTry() {
			continue
		}

		// skip this slot if it is pinned or the CLOCK bit is set
		if latch.pin > 0 {
			if latch.pin&ClockBit > 0 {
				FetchAndAndUint32(&latch.pin, ^ClockBit)
			}
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			continue
		}

		//  update permanent page area in btree from buffer pool
		page := mgr.pagePool[slot]

		if latch.dirty {
			if err := mgr.PageOut(&page, latch.pageNo, true); err != BLTErrOk {
				return nil
			} else {
				latch.dirty = false
				*writes++
			}
		}

		//  unlink our available slot from its hash chain
		if latch.prev > 0 {
			mgr.latchSets[latch.prev].next = latch.next
		} else {
			mgr.hashTable[idx].slot = latch.next
		}

		if latch.next > 0 {
			mgr.latchSets[latch.next].prev = latch.prev
		}

		if mgr.LatchLink(hashIdx, slot, pageNo, loadIt, reads) != BLTErrOk {
			mgr.hashTable[idx].latch.SpinReleaseWrite()
			return nil
		}
		mgr.hashTable[idx].latch.SpinReleaseWrite()

		return latch
	}
}

// UnpinLatch unpins a page in the buffer pool
func (mgr *BufMgrOrgImpl) UnpinLatch(latch *Latchs) {
	if ^latch.pin&ClockBit > 0 {
		FetchAndOrUint32(&latch.pin, ClockBit)
	}
	atomic.AddUint32(&latch.pin, DECREMENT)
}

// NewPage allocate a new page
// returns the page with latched but unlocked
func (mgr *BufMgrOrgImpl) NewPage(set *PageSet, contents *Page, reads *uint, writes *uint) BLTErr {
	// lock allocation page
	mgr.lock.SpinWriteLock()

	// use empty chain first, else allocate empty page
	pageNo := GetID(&mgr.pageZero.chain)
	//fmt.Println("NewPage pageNo: ", pageNo)
	if pageNo > 0 {
		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		if set.latch != nil {
			set.page = mgr.GetRefOfPageAtPool(set.latch)
		} else {
			mgr.err = BLTErrStruct
			return mgr.err
		}

		PutID(&mgr.pageZero.chain, GetID(&set.page.Right))
		mgr.lock.SpinReleaseWrite()
		MemCpyPage(set.page, contents)

		set.latch.dirty = true
		mgr.err = BLTErrOk
		return mgr.err
	}

	pageNo = GetID(mgr.pageZero.AllocRight())
	mgr.pageZero.SetAllocRight(pageNo + 1)

	// unlock allocation latch
	mgr.lock.SpinReleaseWrite()

	// don't load cache from btree page
	set.latch = mgr.PinLatch(pageNo, false, reads, writes)
	if set.latch != nil {
		set.page = mgr.GetRefOfPageAtPool(set.latch)
	} else {
		mgr.err = BLTErrStruct
		return mgr.err
	}

	set.page.Data = make([]byte, mgr.pageDataSize)
	MemCpyPage(set.page, contents)
	set.latch.dirty = true
	mgr.err = BLTErrOk
	return mgr.err
}

// LoadPage find and load page at given level for given key leave page read or write locked as requested
func (mgr *BufMgrOrgImpl) PageFetch(set *PageSet, key []byte, lvl uint8, lock BLTLockMode, reads *uint, writes *uint) uint32 {
	pageNo := RootPage
	prevPage := Uid(0)
	drill := uint8(0xff)
	var slot uint32
	var prevLatch *Latchs

	mode := LockNone
	prevMode := LockNone

	// start at root of btree and drill down
	for pageNo > 0 {
		// determine lock mode of drill level
		if drill == lvl {
			mode = lock
		} else {
			mode = LockRead
		}

		set.latch = mgr.PinLatch(pageNo, true, reads, writes)
		if set.latch == nil {
			return 0
		}

		// obtain access lock using lock chaining with Access mode
		if pageNo > RootPage {
			mgr.PageLock(LockAccess, set.latch)
		}

		set.page = mgr.GetRefOfPageAtPool(set.latch)

		// release & unpin parent page
		if prevPage > 0 {
			mgr.PageUnlock(prevMode, prevLatch)
			mgr.UnpinLatch(prevLatch)
			prevPage = Uid(0)
		}

		// skip Atomic lock on leaf page if already held
		// Note: not supported in this golang implementation
		//if (!drill) {
		//	if (mode & LockAtomic) {
		//		if (pthread_equal( set->latch->atomictid, pthread_self() )) {
		//			mode &= ~LockAtomic;
		//		}
		//	}
		//}

		// obtain mode lock using lock chaining through AccessLock
		mgr.PageLock(mode, set.latch)

		// Note: not supported in this golang implementation
		//if (mode & LockAtomic) {
		//	set->latch->atomictid = pthread_self();
		//}

		if set.page.Free {
			mgr.err = BLTErrStruct
			return 0
		}

		if pageNo > RootPage {
			mgr.PageUnlock(LockAccess, set.latch)
		}

		// re-read and re-lock root after determining actual level of root
		if set.page.Lvl != drill {
			if set.latch.pageNo != RootPage {
				mgr.err = BLTErrStruct
				return 0
			}

			drill = set.page.Lvl

			if lock != LockRead && drill == lvl {
				mgr.PageUnlock(mode, set.latch)
				mgr.UnpinLatch(set.latch)
				continue
			}
		}

		prevPage = set.latch.pageNo
		prevLatch = set.latch
		prevMode = mode

		//  find key on page at this level
		//  and descend to requested level
		if set.page.Kill {
			goto sliderRight
		}

		slot = set.page.FindSlot(key)
		if slot > 0 {
			if drill == lvl {
				return slot
			}

			for set.page.Dead(slot) {
				if slot < set.page.Cnt {
					slot++
					continue
				} else {
					goto sliderRight
				}
			}

			pageNo = GetIDFromValue(set.page.Value(slot))
			drill--
			continue
		}

	sliderRight: // slide right into next page
		pageNo = GetID(&set.page.Right)
	}

	// return error on end of right chain
	mgr.err = BLTErrStruct
	return 0
}

// FreePage
//
// return page to free list
// page must be delete and write locked
func (mgr *BufMgrOrgImpl) PageFree(set *PageSet) {

	// lock allocation page
	mgr.lock.SpinWriteLock()

	// store chain
	set.page.Right = mgr.pageZero.chain
	PutID(&mgr.pageZero.chain, set.latch.pageNo)
	set.latch.dirty = true
	set.page.Free = true

	// unlock released page
	mgr.PageUnlock(LockDelete, set.latch)
	mgr.PageUnlock(LockWrite, set.latch)
	mgr.UnpinLatch(set.latch)

	// unlock allocation page
	mgr.lock.SpinReleaseWrite()
}

// LockPage
//
// place write, read, or parent lock on requested page_no
func (mgr *BufMgrOrgImpl) PageLock(mode BLTLockMode, latch *Latchs) {
	switch mode {
	case LockRead:
		latch.readWr.ReadLock()
	case LockWrite:
		latch.readWr.WriteLock()
	case LockAccess:
		latch.access.ReadLock()
	case LockDelete:
		latch.access.WriteLock()
	case LockParent:
		latch.parent.WriteLock()
		//case LockAtomic: // Note: not supported in this golang implementation
	}
}

func (mgr *BufMgrOrgImpl) PageUnlock(mode BLTLockMode, latch *Latchs) {
	switch mode {
	case LockRead:
		latch.readWr.ReadRelease()
	case LockWrite:
		latch.readWr.WriteRelease()
	case LockAccess:
		latch.access.ReadRelease()
	case LockDelete:
		latch.access.WriteRelease()
	case LockParent:
		latch.parent.WriteRelease()
		//case LockAtomic: // Note: not supported in this golang implementation
	}
}

func (mgr *BufMgrOrgImpl) GetPageDataSize() uint32 {
	return mgr.pageDataSize
}
func (mgr *BufMgrOrgImpl) GetLatchSets() []Latchs {
	return mgr.latchSets
}
func (mgr *BufMgrOrgImpl) GetPageBits() uint8 {
	return mgr.pageBits
}
func (mgr *BufMgrOrgImpl) GetPageZero() *PageZero {
	return &mgr.pageZero
}
func (mgr *BufMgrOrgImpl) GetPagePool() []Page {
	return mgr.pagePool
}
