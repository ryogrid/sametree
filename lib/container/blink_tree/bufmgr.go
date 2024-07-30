package blink_tree

type BufMgr interface {
	PageIn(page *Page, pageNo Uid) BLTErr
	PageOut(page *Page, pageNo Uid, isDirty bool) BLTErr
	Close()
	PoolAudit()
	LatchLink(hashIdx uint, slot uint, pageNo Uid, loadIt bool, reads *uint) BLTErr
	GetRefOfPageAtPool(latch *Latchs) *Page
	PinLatch(pageNo Uid, loadIt bool, reads *uint, writes *uint) *Latchs
	UnpinLatch(latch *Latchs)
	NewPage(set *PageSet, contents *Page, reads *uint, writes *uint) BLTErr
	PageFetch(set *PageSet, key []byte, lvl uint8, lock BLTLockMode, reads *uint, writes *uint) uint32
	PageFree(set *PageSet)
	PageLock(mode BLTLockMode, latch *Latchs)
	PageUnlock(mode BLTLockMode, latch *Latchs)
	GetPageDataSize() uint32
	GetLatchSets() []Latchs
	GetPageBits() uint8
	GetPageZero() *PageZero
	GetPagePool() []Page
}
