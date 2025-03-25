package timingwheel

import (
	"container/list"
	"sync"
	"sync/atomic"
	"unsafe"
)

// Timer represents a single event. When the Timer expires, the given
// task will be executed.
type Timer struct {
	// 过期时间，以毫秒为单位
	expiration int64
	// 定时器触发时要执行的任务函数
	task func() // 定时器触发时要执行的任务函数

	// The bucket that holds the list to which this timer's element belongs.
	//
	// NOTE: This field may be updated and read concurrently,
	// through Timer.Stop() and Bucket.Flush().
	// type: *bucket  指向持有该定时器的桶，使用unsafe.Pointer是为了支持原子操作
	b unsafe.Pointer

	// The timer's element.
	// 定时器在链表中的元素指针
	element *list.Element
}

// 原子地获取定时器所在的桶
func (t *Timer) getBucket() *bucket {
	return (*bucket)(atomic.LoadPointer(&t.b))
}

// 原子地设置定时器所在的桶
func (t *Timer) setBucket(b *bucket) {
	atomic.StorePointer(&t.b, unsafe.Pointer(b))
}

// Stop prevents the Timer from firing. It returns true if the call
// stops the timer, false if the timer has already expired or been stopped.
//
// If the timer t has already expired and the t.task has been started in its own
// goroutine; Stop does not wait for t.task to complete before returning. If the caller
// needs to know whether t.task is completed, it must coordinate with t.task explicitly.
//
// 这个方法会尝试从桶中移除定时器。如果定时器已经被移动到另一个桶中，它会重新获取当前桶并再次尝试移除，直到桶变为 nil 为止。
func (t *Timer) Stop() bool {
	stopped := false
	for b := t.getBucket(); b != nil; b = t.getBucket() {
		// If b.Remove is called just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		// this may fail to remove t due to the change of t's bucket.
		stopped = b.Remove(t)

		// Thus, here we re-get t's possibly new bucket (nil for case 1, or ab (non-nil) for case 2),
		// and retry until the bucket becomes nil, which indicates that t has finally been removed.
	}
	return stopped
}

// bucket 是时间轮中的一个槽，用于存储到期时间相近的一组定时器。
type bucket struct {
	// 64-bit atomic operations require 64-bit alignment, but 32-bit
	// compilers do not ensure it. So we must keep the 64-bit field
	// as the first field of the struct.
	//
	// For more explanations, see https://golang.org/pkg/sync/atomic/#pkg-note-BUG
	// and https://go101.org/article/memory-layout.html.
	expiration int64

	mu     sync.Mutex
	timers *list.List
}

// 创建一个新的桶
func newBucket() *bucket {
	return &bucket{
		timers:     list.New(),
		expiration: -1,
	}
}

// 获取桶的过期时间
func (b *bucket) Expiration() int64 {
	return atomic.LoadInt64(&b.expiration)
}

// 设置桶的过期时间
// 如果设置的过期时间与当前值不同，则返回 true
func (b *bucket) SetExpiration(expiration int64) bool {
	return atomic.SwapInt64(&b.expiration, expiration) != expiration
}

// 向桶中添加定时器
// 将定时器添加到桶的链表末尾，并设置定时器的桶指针和链表元素指针。
func (b *bucket) Add(t *Timer) {
	b.mu.Lock()

	e := b.timers.PushBack(t)
	t.setBucket(b)
	t.element = e

	b.mu.Unlock()
}

// 如果定时器不在当前桶中，则返回 false；否则，从链表中移除定时器并清除其桶和元素引用。
func (b *bucket) remove(t *Timer) bool {
	if t.getBucket() != b {
		// If remove is called from within t.Stop, and this happens just after the timing wheel's goroutine has:
		//     1. removed t from b (through b.Flush -> b.remove)
		//     2. moved t from b to another bucket ab (through b.Flush -> b.remove and ab.Add)
		//     1. 时间轮的协程已经通过 b.Flush -> b.remove 将定时器 t 从桶 b 中移除了
		//	   2. 时间轮的协程已经通过 b.Flush -> b.remove 将定时器 t 从桶 b 中移除，并通过 ab.Add 将其添加到另一个桶 ab 中
		// then t.getBucket will return nil for case 1, or ab (non-nil) for case 2.
		// In either case, the returned value does not equal to b.
		return false
	}
	b.timers.Remove(t.element)
	t.setBucket(nil)
	t.element = nil
	return true
}

// 公开方法，从桶中移除定时器. 带有互斥锁保护的 remove() 版本。
func (b *bucket) Remove(t *Timer) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.remove(t)
}

// 清空桶并重新插入定时器
// 这个方法会遍历桶中的所有定时器，移除它们，然后使用提供的 reinsert 函数重新处理这些定时器。最后，将桶的过期时间重置为 -1。
func (b *bucket) Flush(reinsert func(*Timer)) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for e := b.timers.Front(); e != nil; {
		next := e.Next()

		t := e.Value.(*Timer)
		b.remove(t)
		// Note that this operation will either execute the timer's task, or
		// insert the timer into another bucket belonging to a lower-level wheel.
		//
		// In either case, no further lock operation will happen to b.mu.
		reinsert(t)

		e = next
	}

	b.SetExpiration(-1)
}
