package timingwheel

import (
	"errors"
	"sync/atomic"
	"time"
	"unsafe"
)

// TimingWheel is an implementation of Hierarchical Timing Wheels.
// 第一层时间轮处理短期任务
// 当任务的到期时间超出第一层时间轮的范围时，会被放入溢出轮（overflowWheel）
// 溢出轮的每个刻度（tick）等于第一层轮的整个间隔（interval）
type TimingWheel struct {
	tick      int64 // 时间刻度，单位是毫秒
	wheelSize int64 // 时间轮的大小（槽数量）

	interval    int64       // 整个时间轮表示的时间跨度，等于 tick * wheelSize，单位是毫秒
	currentTime int64       // 当前时间点，单位是毫秒
	buckets     []*bucket   // 存储定时器的桶数组
	queue       *DelayQueue // 用于排序和提取到期任务

	// The higher-level overflow wheel.
	//
	// NOTE: This field may be updated and read concurrently, through Add().
	overflowWheel unsafe.Pointer // type: *TimingWheel 指向更高层级的时间轮

	exitC     chan struct{}    // 退出信号通道
	waitGroup waitGroupWrapper // 等待组，用于同步退出
}

// NewTimingWheel creates an instance of TimingWheel with the given tick and wheelSize.
// 验证刻度必须大于或等于1毫秒后，获取当前时间（毫秒）再调用内部函数创建时间轮实例
func NewTimingWheel(tick time.Duration, wheelSize int64) *TimingWheel {
	tickMs := int64(tick / time.Millisecond)
	if tickMs <= 0 {
		panic(errors.New("tick must be greater than or equal to 1ms"))
	}

	startMs := timeToMs(time.Now().UTC())

	return newTimingWheel(
		tickMs,
		wheelSize,
		startMs,
		NewDelayqueue(int(wheelSize)),
	)
}

// newTimingWheel is an internal helper function that really creates an instance of TimingWheel.
func newTimingWheel(tickMs int64, wheelSize int64, startMs int64, queue *DelayQueue) *TimingWheel {
	buckets := make([]*bucket, wheelSize) // 创建一个桶数组，长度为 wheelSize
	for i := range buckets {
		buckets[i] = newBucket()
	}
	return &TimingWheel{
		tick:        tickMs,
		wheelSize:   wheelSize,
		currentTime: truncate(startMs, tickMs), //使用 truncate 函数将起始时间向下取整到最接近的刻度
		interval:    tickMs * wheelSize,        // 计算时间轮的总间隔（刻度 × 轮盘大小）
		buckets:     buckets,
		queue:       queue,
		exitC:       make(chan struct{}),
	}
}

// add inserts the timer t into the current timing wheel.
func (tw *TimingWheel) add(t *Timer) bool {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	// 时间轮（TimingWheel）并不是精确到纳秒的定时器，它最小的时间粒度是 tick。
	// 任何任务的执行时间都是按照 tick 为单位进行调度，不会精确到更小的时间点
	if t.expiration < currentTime+tw.tick { // 如果定时器已过期（到期时间早于当前时间+一个刻度），直接返回 false
		// Already expired
		return false
	} else if t.expiration < currentTime+tw.interval { // 如果定时器在时间轮的间隔内，则将其放入相应的桶中
		// Put it into its own bucket
		virtualID := t.expiration / tw.tick     // 计算定时器所属的逻辑槽位编号
		b := tw.buckets[virtualID%tw.wheelSize] // 在当前时间轮中的实际 bucket 位置
		b.Add(t)                                // 将定时器添加到相应的桶中

		// Set the bucket expiration time
		if b.SetExpiration(virtualID * tw.tick) { // 桶只有在过期时间真正改变时才会被入队 防止同一个桶在同一轮循环中被重复入队
			// The bucket needs to be enqueued since it was an expired bucket.
			// We only need to enqueue the bucket when its expiration time has changed,
			// i.e. the wheel has advanced and this bucket get reused with a new expiration.
			// Any further calls to set the expiration within the same wheel cycle will
			// pass in the same value and hence return false, thus the bucket with the
			// same expiration will not be enqueued multiple times.
			tw.queue.Offer(b, b.Expiration()) // 插入delayqueue中
		}

		return true
	} else { // 如果定时器超出当前时间轮范围，放入溢出时间轮
		// Out of the interval. Put it into the overflow wheel
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel == nil { // 如果溢出时间轮为空，则创建一个新时间轮并替换当前时间轮
			atomic.CompareAndSwapPointer(
				&tw.overflowWheel,
				nil,
				unsafe.Pointer(newTimingWheel(
					tw.interval,  // 溢出时间轮的刻度，等于当前时间轮的刻度*当前时间轮的大小
					tw.wheelSize, // 溢出时间轮的大小，等于当前时间轮的大小
					currentTime,  // 溢出时间轮的起始时间，等于当前时间轮的起始时间
					tw.queue,
				)),
			)
			overflowWheel = atomic.LoadPointer(&tw.overflowWheel)
		}
		return (*TimingWheel)(overflowWheel).add(t)
	}
}

// addOrRun inserts the timer t into the current timing wheel, or run the
// timer's task if it has already expired.
func (tw *TimingWheel) addOrRun(t *Timer) {
	if !tw.add(t) {
		// Already expired

		// Like the standard time.AfterFunc (https://golang.org/pkg/time/#AfterFunc),
		// always execute the timer's task in its own goroutine.
		go t.task()
	}
}

// 更新当前时间，并尝试更新溢出时间轮的当前时间
func (tw *TimingWheel) advanceClock(expiration int64) {
	currentTime := atomic.LoadInt64(&tw.currentTime)
	if expiration >= currentTime+tw.tick { // 如果新到期时间大于当前时间+一个刻度，则更新当前时间并尝试更新溢出时间轮的当前时间
		currentTime = truncate(expiration, tw.tick)
		atomic.StoreInt64(&tw.currentTime, currentTime)

		// Try to advance the clock of the overflow wheel if present
		overflowWheel := atomic.LoadPointer(&tw.overflowWheel)
		if overflowWheel != nil {
			(*TimingWheel)(overflowWheel).advanceClock(currentTime)
		}
	}
}

// Start starts the current timing wheel.
func (tw *TimingWheel) Start() {
	tw.waitGroup.Wrap(func() { // 使用waitGroup启动一个 goroutine，用于从 delayqueue 中获取到期的定时器，并调用相应的任务函数
		tw.queue.Poll(tw.exitC, func() int64 {
			return timeToMs(time.Now().UTC())
		})
	})

	tw.waitGroup.Wrap(func() {
		for {
			select {
			case elem := <-tw.queue.C:
				b := elem.(*bucket)
				tw.advanceClock(b.Expiration())
				b.Flush(tw.addOrRun)
			case <-tw.exitC:
				return
			}
		}
	})
}

// Stop stops the current timing wheel.
//
// If there is any timer's task being running in its own goroutine, Stop does
// not wait for the task to complete before returning. If the caller needs to
// know whether the task is completed, it must coordinate with the task explicitly.
func (tw *TimingWheel) Stop() {
	close(tw.exitC)
	tw.waitGroup.Wait()
}

// AfterFunc waits for the duration to elapse and then calls f in its own goroutine.
// It returns a Timer that can be used to cancel the call using its Stop method.
func (tw *TimingWheel) AfterFunc(d time.Duration, f func()) *Timer {
	t := &Timer{
		expiration: timeToMs(time.Now().UTC().Add(d)),
		task:       f,
	}
	tw.addOrRun(t)
	return t
}

// Scheduler determines the execution plan of a task.
type Scheduler interface {
	// Next returns the next execution time after the given (previous) time.
	// It will return a zero time if no next time is scheduled.
	//
	// All times must be UTC.
	Next(time.Time) time.Time
}

// ScheduleFunc calls f (in its own goroutine) according to the execution
// plan scheduled by s. It returns a Timer that can be used to cancel the
// call using its Stop method.
//
// If the caller want to terminate the execution plan halfway, it must
// stop the timer and ensure that the timer is stopped actually, since in
// the current implementation, there is a gap between the expiring and the
// restarting of the timer. The wait time for ensuring is short since the
// gap is very small.
//
// Internally, ScheduleFunc will ask the first execution time (by calling
// s.Next()) initially, and create a timer if the execution time is non-zero.
// Afterwards, it will ask the next execution time each time f is about to
// be executed, and f will be called at the next execution time if the time
// is non-zero.
func (tw *TimingWheel) ScheduleFunc(s Scheduler, f func()) (t *Timer) {
	expiration := s.Next(time.Now().UTC())
	if expiration.IsZero() {
		// No time is scheduled, return nil.
		return
	}

	t = &Timer{
		expiration: timeToMs(expiration),
		task: func() {
			// Schedule the task to execute at the next time if possible.
			expiration := s.Next(msToTime(t.expiration))
			if !expiration.IsZero() {
				t.expiration = timeToMs(expiration)
				tw.addOrRun(t)
			}

			// Actually execute the task.
			f()
		},
	}
	tw.addOrRun(t)

	return
}
