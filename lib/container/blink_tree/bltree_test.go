package blink_tree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ryogrid/sametree/lib/storage/buffer"
	"github.com/ryogrid/sametree/lib/storage/disk"
)

func TestBLTree_collapseRoot(t *testing.T) {
	_ = os.Remove("data/collapse_root_test.db")

	type fields struct {
		mgr BufMgr
	}
	tests := []struct {
		name   string
		fields fields
		want   BLTErr
	}{
		{
			name: "collapse root",
			fields: fields{
				mgr: NewBufMgr("data/collapse_root_test.db", 13, 20),
			},
			want: BLTErrOk,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tree := NewBLTree(tt.fields.mgr)
			for _, key := range [][]byte{
				{1, 1, 1, 1},
				{1, 1, 1, 2},
			} {
				if err := tree.insertKey(key, 0, [BtId]byte{1}, true); err != BLTErrOk {
					t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
				}

			}
			if rootAct := tree.mgr.GetPagePool()[RootPage].Act; rootAct != 1 {
				t.Errorf("rootAct = %v, want %v", rootAct, 1)
			}
			if childAct := tree.mgr.GetPagePool()[RootPage+1].Act; childAct != 3 {
				t.Errorf("childAct = %v, want %v", childAct, 3)
			}
			var set PageSet
			set.latch = tree.mgr.PinLatch(RootPage, true, &tree.reads, &tree.writes)
			set.page = tree.mgr.MapPage(set.latch)
			if got := tree.collapseRoot(&set); got != tt.want {
				t.Errorf("collapseRoot() = %v, want %v", got, tt.want)
			}

			if rootAct := tree.mgr.GetPagePool()[RootPage].Act; rootAct != 3 {
				t.Errorf("after collapseRoot rootAct = %v, want %v", rootAct, 3)
			}

			if !tree.mgr.GetPagePool()[RootPage+1].Free {
				t.Errorf("after collapseRoot childFree = %v, want %v", false, true)
			}

		})
	}
}

func TestBLTree_cleanPage_full_page(t *testing.T) {
	_ = os.Remove("data/bltree_clean_page.db")
	mgr := NewBufMgr("data/bltree_clean_page.db", 15, 16*7)
	bltree := NewBLTree(mgr)

	f, err := os.OpenFile("testdata/page_for_clean", os.O_RDWR, 0666)
	if err != nil {
		t.Fatal(err)
	}

	// ファイルの数値をすべてバイト配列に読み込む
	// ファイルの内容はすべて文字列で、空白文字で区切られている
	var data []byte
	for {
		var b byte
		_, err := fmt.Fscanf(f, "%d", &b)
		if err != nil {
			break
		}
		data = append(data, b)
	}
	fmt.Printf("size: %v\n", len(data))

	set := PageSet{
		page:  NewPage(mgr.GetPageDataSize()),
		latch: &LatchSet{},
	}
	copy(set.page.Data, data)
	set.page.PageHeader = PageHeader{
		Cnt:     1214,
		Act:     1170,
		Min:     7302,
		Garbage: 6720,
		Bits:    15,
		Free:    false,
		Lvl:     0,
		Kill:    false,
		Right:   [BtId]byte{0, 0, 0, 0, 1, 74},
	}
	res := bltree.cleanPage(&set, 8, 439, BtId)
	if res != 0 {
		t.Errorf("cleanPage() = %v, want %v", res, 0)
	}
}

func TestBLTree_insert_and_find(t *testing.T) {
	mgr := NewBufMgr("data/bltree_insert_and_find.db", 13, 20)
	bltree := NewBLTree(mgr)
	if valLen, _, _ := bltree.findKey([]byte{1, 1, 1, 1}, BtId); valLen >= 0 {
		t.Errorf("findKey() = %v, want %v", valLen, -1)
	}

	if err := bltree.insertKey([]byte{1, 1, 1, 1}, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
	}

	_, foundKey, _ := bltree.findKey([]byte{1, 1, 1, 1}, BtId)
	if bytes.Compare(foundKey, []byte{1, 1, 1, 1}) != 0 {
		t.Errorf("findKey() = %v, want %v", foundKey, []byte{1, 1, 1, 1})
	}
}

func TestBLTree_insert_and_find_samehada(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	bpm := buffer.NewBufferPoolManager(poolSize, dm)

	os.Remove("data/bltree_insert_and_find_samehada.db")

	mgr := NewBufMgrSamehada("data/bltree_insert_and_find_samehada.db", 12, 20, bpm, nil)
	bltree := NewBLTree(mgr)
	if valLen, _, _ := bltree.findKey([]byte{1, 1, 1, 1}, BtId); valLen >= 0 {
		t.Errorf("findKey() = %v, want %v", valLen, -1)
	}

	if err := bltree.insertKey([]byte{1, 1, 1, 1}, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
	}

	_, foundKey, _ := bltree.findKey([]byte{1, 1, 1, 1}, BtId)
	if bytes.Compare(foundKey, []byte{1, 1, 1, 1}) != 0 {
		t.Errorf("findKey() = %v, want %v", foundKey, []byte{1, 1, 1, 1})
	}
}

func TestBLTree_insert_and_find_many(t *testing.T) {
	_ = os.Remove(`data/bltree_insert_and_find_many.db`)
	mgr := NewBufMgr("data/bltree_insert_and_find_many.db", 13, 48)
	bltree := NewBLTree(mgr)

	num := uint64(160000)

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.findKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("findKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_insert_and_find_concurrently_org(t *testing.T) {
	_ = os.Remove(`data/insert_and_find_concurrently.db`)
	mgr := NewBufMgr("data/insert_and_find_concurrently.db", 13, 16*7)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	insertAndFindConcurrently(t, 7, mgr, keys)
}

func TestBLTree_insert_and_find_concurrently_samehada(t *testing.T) {
	_ = os.Remove("data/insert_and_find_concurrently_samehada.db")
	_ = os.Remove("TestBLTree_insert_and_find_concurrently_samehada.db")

	poolSize := uint32(300)

	//dm := disk.NewDiskManagerImpl("TestBLTree_insert_and_find_concurrently_samehada.db")
	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_insert_and_find_concurrently_samehada.db")
	bpm := buffer.NewBufferPoolManager(poolSize, dm)

	mgr := NewBufMgrSamehada("data/insert_and_find_concurrently_samehada.db", 12, 16*7, bpm, nil)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	insertAndFindConcurrently(t, 7, mgr, keys)
}

func TestBLTree_insert_and_find_concurrently_by_little_endian(t *testing.T) {
	_ = os.Remove(`data/insert_and_find_concurrently_by_little_endian.db`)
	mgr := NewBufMgr("data/insert_and_find_concurrently_by_little_endian.db", 13, 16*7)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	insertAndFindConcurrently(t, 7, mgr, keys)
}

func insertAndFindConcurrently(t *testing.T, routineNum int, mgr BufMgr, keys [][]byte) {
	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	keyTotal := len(keys)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.insertKey(keys[i], 0, [BtId]byte{}, true); err != BLTErrOk {
					t.Errorf("in goroutine%d insertKey() = %v, want %v", n, err, BLTErrOk)
				}

				if _, foundKey, _ := bltree.findKey(keys[i], BtId); bytes.Compare(foundKey, keys[i]) != 0 {
					t.Errorf("in goroutine%d findKey() = %v, want %v", n, foundKey, keys[i])
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if _, foundKey, _ := bltree.findKey(keys[i], BtId); bytes.Compare(foundKey, keys[i]) != 0 {
					t.Errorf("findKey() = %v, want %v, i = %d", foundKey, keys[i], i)
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_delete(t *testing.T) {
	mgr := NewBufMgr("data/bltree_delete.db", 13, 20)
	bltree := NewBLTree(mgr)

	key := []byte{1, 1, 1, 1}

	if err := bltree.insertKey(key, 0, [BtId]byte{0, 0, 0, 0, 0, 1}, true); err != BLTErrOk {
		t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
	}

	if err := bltree.deleteKey(key, 0); err != BLTErrOk {
		t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
	}

	if found, _, _ := bltree.findKey(key, BtId); found != -1 {
		t.Errorf("findKey() = %v, want %v", found, -1)
	}
}

func TestBLTree_deleteMany(t *testing.T) {
	_ = os.Remove(`data/bltree_delete_many.db`)
	mgr := NewBufMgr("data/bltree_delete_many.db", 13, 16*7)
	bltree := NewBLTree(mgr)

	keyTotal := 160000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.insertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
		if i%2 == 0 {
			if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
				t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
			}
		}
	}

	for i := range keys {
		if i%2 == 0 {
			if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
				t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
			}
		} else {
			if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
				t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
			}
		}
	}
}

func TestBLTree_deleteMany_samehada(t *testing.T) {
	_ = os.Remove(`data/bltree_delete_many_samehada.db`)

	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteMany_samehada.db")
	bpm := buffer.NewBufferPoolManager(poolSize, dm)

	mgr := NewBufMgrSamehada("data/bltree_delete_many_samehada.db", 12, 16*7, bpm, nil)
	bltree := NewBLTree(mgr)

	keyTotal := 160000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.insertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
		if i%2 == 0 {
			if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
				t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
			}
		}
	}

	for i := range keys {
		if i%2 == 0 {
			if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
				t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
			}
		} else {
			if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
				t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
			}
		}
	}
}

func TestBLTree_deleteAll(t *testing.T) {
	_ = os.Remove(`data/bltree_delete_all.db`)
	mgr := NewBufMgr("data/bltree_delete_all.db", 13, 16*7)
	bltree := NewBLTree(mgr)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.insertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := range keys {
		if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
			t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
		}
		if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
			t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
		}
	}
}

func TestBLTree_deleteAll_samehada(t *testing.T) {
	_ = os.Remove(`data/bltree_delete_all.db`)
	_ = os.Remove("TestBLTree_deleteAll_samehada.db")

	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteAll_samehada.db")
	bpm := buffer.NewBufferPoolManager(poolSize, dm)
	mgr := NewBufMgrSamehada("data/bltree_delete_all.db", 12, 16*7, bpm, nil)
	bltree := NewBLTree(mgr)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.insertKey(keys[i], 0, [BtId]byte{0, 0, 0, 0, 0, 0}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := range keys {
		if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
			t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
		}
		if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
			t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
		}
	}
}

func TestBLTree_deleteManyConcurrently(t *testing.T) {
	_ = os.Remove("data/bltree_delete_many_concurrently.db")
	mgr := NewBufMgr("data/bltree_delete_many_concurrently.db", 13, 16*7)

	keyTotal := 1600000
	routineNum := 7

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.insertKey(keys[i], 0, [BtId]byte{}, true); err != BLTErrOk {
					t.Errorf("in goroutine%d insertKey() = %v, want %v", n, err, BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
						t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
						t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("findKey() != -1")
					}
				} else {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
						t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("findKey() != 6")
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
						t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
					}
				} else {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
						t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_deleteManyConcurrently_samehada(t *testing.T) {
	_ = os.Remove("data/bltree_delete_many_concurrently.db")
	_ = os.Remove("TestBLTree_deleteManyConcurrently_samehada.db")

	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteManyConcurrently_samehada.db")
	bpm := buffer.NewBufferPoolManager(poolSize, dm)
	mgr := NewBufMgrSamehada("data/bltree_delete_many_concurrently.db", 12, 16*7, bpm, nil)

	keyTotal := 1600000
	routineNum := 7

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.insertKey(keys[i], 0, [BtId]byte{}, true); err != BLTErrOk {
					t.Errorf("in goroutine%d insertKey() = %v, want %v", n, err, BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.deleteKey(keys[i], 0); err != BLTErrOk {
						t.Errorf("deleteKey() = %v, want %v", err, BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
						t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("findKey() != -1")
					}
				} else {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
						t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("findKey() != 6")
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != -1 {
						t.Errorf("findKey() = %v, want %v, key %v", found, -1, keys[i])
					}
				} else {
					if found, _, _ := bltree.findKey(keys[i], BtId); found != 6 {
						t.Errorf("findKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_restart(t *testing.T) {
	_ = os.Remove(`data/bltree_restart.db`)
	mgr := NewBufMgr("data/bltree_restart.db", 13, 48)
	bltree := NewBLTree(mgr)

	firstNum := uint64(1000)

	for i := uint64(0); i <= firstNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	mgr.Close()
	mgr = NewBufMgr("data/bltree_restart.db", 15, 48)
	bltree = NewBLTree(mgr)

	secondNum := uint64(2000)

	for i := firstNum; i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.findKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("findKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_restart_samehada(t *testing.T) {
	_ = os.Remove(`data/bltree_restart_samehada.db`)
	_ = os.Remove("TestBLTree_restart_samehada.db")

	poolSize := uint32(100)

	dm := disk.NewDiskManagerImpl("TestBLTree_restart_samehada.db")
	bpm := buffer.NewBufferPoolManager(poolSize, dm)

	mgr := NewBufMgrSamehada("data/bltree_restart_samehada.db", 12, 48, bpm, nil)
	bltree := NewBLTree(mgr)

	firstNum := uint64(1000)

	for i := uint64(0); i <= firstNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	mgr.Close()
	pageZeroShId := mgr.(*BufMgrSamehadaImpl).GetMappedShPageIdOfPageZero()
	bpm.FlushAllPages()
	dm.ShutDown()

	dm = disk.NewDiskManagerImpl("TestBLTree_restart_samehada.db")
	bpm = buffer.NewBufferPoolManager(poolSize, dm)
	mgr = NewBufMgrSamehada("data/bltree_restart_samehada.db", 12, 48, bpm, pageZeroShId)
	bltree = NewBLTree(mgr)

	secondNum := uint64(2000)

	for i := firstNum; i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.insertKey(bs, 0, [BtId]byte{}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	for i := uint64(0); i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.findKey(bs, BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("findKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_insert_and_range_scan_samehada(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	bpm := buffer.NewBufferPoolManager(poolSize, dm)

	os.Remove("data/bltree_insert_and_range_scan_samehada.db")

	mgr := NewBufMgrSamehada("TestBLTree_insert_and_range_scan_samehada.db", 12, 20, bpm, nil)
	bltree := NewBLTree(mgr)

	keyTotal := 10
	keys := make([][]byte, keyTotal)
	for i := 1; i < keyTotal; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		keys[i-1] = key
	}

	// generate shuffled keys
	keysRandom := make([][]byte, keyTotal-1)
	copy(keysRandom, keys)
	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	randGen.Shuffle(len(keysRandom), func(i, j int) { keysRandom[i], keysRandom[j] = keysRandom[j], keysRandom[i] })

	// insert in shuffled order
	for i := 0; i < keyTotal-1; i++ {
		key := keysRandom[i]
		val := make([]byte, 4)
		binary.LittleEndian.PutUint32(val, uint32(i))
		if err := bltree.insertKey(key, 0, [BtId]byte{val[3], val[2], val[1], val[0], 0, 1}, true); err != BLTErrOk {
			t.Errorf("insertKey() = %v, want %v", err, BLTErrOk)
		}
	}

	// range scan and check keys are sorted
	itrCnt := 0
	bltree.err = -1
	for slot := bltree.startKey([]byte{0, 0, 0, 0, 0, 0, 0, 0}); bltree.err != BLTErrOk; slot = bltree.nextKey(slot) {
		slotType := bltree.cursor.Typ(slot)
		if slotType != Unique {
			continue
		}
		key := bltree.cursor.Key(slot)
		if bytes.Compare(key, keys[itrCnt]) != 0 {
			t.Errorf("key = %v, want %v", key, keys[itrCnt])
		}
		keyBuf := bytes.NewBuffer(key)
		readKey := uint64(math.MaxUint64)
		binary.Read(keyBuf, binary.LittleEndian, &readKey)
		fmt.Println("readKey: ", readKey)

		val := bltree.cursor.Key(slot)
		valBuf := bytes.NewBuffer(val)
		readVal := uint32(math.MaxUint32)
		binary.Read(valBuf, binary.LittleEndian, &readVal)
		fmt.Println("readVal: ", readKey)
		itrCnt++
	}
}
