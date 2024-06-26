package blink_tree

type BufMgr interface {
	ReadPage(page **Page, pageNo Uid) BLTErr
	WritePage(page *Page, pageNo Uid) BLTErr
	Close()
	PoolAudit()
	LatchLink(hashIdx uint, slot uint, pageNo Uid, loadIt bool, reads *uint) BLTErr
	MapPage(latch *LatchSet) *Page
	PinLatch(pageNo Uid, loadIt bool, reads *uint, writes *uint) *LatchSet
	UnpinLatch(latch *LatchSet)
	NewPage(set *PageSet, contents *Page, reads *uint, writes *uint) BLTErr
	LoadPage(set *PageSet, key []byte, lvl uint8, lock BLTLockMode, reads *uint, writes *uint) uint32
	FreePage(set *PageSet)
	LockPage(mode BLTLockMode, latch *LatchSet)
	UnlockPage(mode BLTLockMode, latch *LatchSet)
	GetPageDataSize() uint32
	GetLatchSets() []LatchSet
	GetPageBits() uint8
	GetPageZero() *PageZero
	GetPagePool() []Page
}
