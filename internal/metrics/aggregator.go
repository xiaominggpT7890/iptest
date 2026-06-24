package metrics

import (
	"sync"
	"sync/atomic"
)

// Aggregator 聚合任务实时指标:线程安全
type Aggregator struct {
	mu sync.RWMutex

	totalReqs atomic.Int64
	success   atomic.Int64
	failed    atomic.Int64

	totalLatencyMs atomic.Int64
	latencyCount   atomic.Int64

	ips       map[string]int64
	countries map[string]int64

	ipCountry map[string]string

	winSize int
	winBuf  []string
	winPos  int
	winFull bool
	winHits int
	winSeen map[string]int

	errKinds map[string]int64
}

func NewAggregator(winSize int) *Aggregator {
	if winSize <= 0 {
		winSize = 1000
	}
	return &Aggregator{
		ips:       make(map[string]int64, 16384),
		countries: make(map[string]int64, 64),
		ipCountry: make(map[string]string, 16384),
		winSize:   winSize,
		winBuf:    make([]string, winSize),
		winSeen:   make(map[string]int, winSize),
		errKinds:  make(map[string]int64, 8),
	}
}

func (a *Aggregator) ObserveSuccess(ip, country string, latencyMs int64) {
	a.totalReqs.Add(1)
	a.success.Add(1)
	a.totalLatencyMs.Add(latencyMs)
	a.latencyCount.Add(1)
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, seen := a.ips[ip]; !seen {
		a.ipCountry[ip] = country
		if country != "" {
			a.countries[country]++
		}
	}
	a.ips[ip]++
	old := a.winBuf[a.winPos]
	if old != "" {
		a.winSeen[old]--
		if a.winSeen[old] <= 0 {
			delete(a.winSeen, old)
		}
	} else if !a.winFull && a.winPos == a.winSize-1 {
		a.winFull = true
	}
	a.winBuf[a.winPos] = ip
	a.winSeen[ip]++
	a.winPos = (a.winPos + 1) % a.winSize
	a.recalcHits()
}

func (a *Aggregator) ObserveFailure(kind string, latencyMs int64) {
	a.totalReqs.Add(1)
	a.failed.Add(1)
	if latencyMs > 0 {
		a.totalLatencyMs.Add(latencyMs)
		a.latencyCount.Add(1)
	}
	a.mu.Lock()
	a.errKinds[kind]++
	a.mu.Unlock()
}

func (a *Aggregator) recalcHits() {
	winLen := a.winSize
	if !a.winFull {
		winLen = a.winPos
	}
	if winLen <= 0 {
		a.winHits = 0
		return
	}
	a.winHits = winLen - len(a.winSeen)
}

// HitRate 当前滑动窗口的命中率 [0,1]
func (a *Aggregator) HitRate() float64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	winLen := a.winSize
	if !a.winFull {
		winLen = a.winPos
	}
	if winLen <= 0 {
		return 0
	}
	return float64(a.winHits) / float64(winLen)
}

// Snapshot 返回当前快照
type Snapshot struct {
	TotalReqs       int64            `json:"total_reqs"`
	Success         int64            `json:"success"`
	Failed          int64            `json:"failed"`
	UniqueIPs       int64            `json:"unique_ips"`
	UniqueCountries int64            `json:"unique_countries"`
	HitRateWindow   float64          `json:"hit_rate"`
	AvgLatencyMs    float64          `json:"avg_latency_ms"`
	ErrKinds        map[string]int64 `json:"err_kinds"`
}

func (a *Aggregator) Snapshot() Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	winLen := a.winSize
	if !a.winFull {
		winLen = a.winPos
	}
	var hr float64
	if winLen > 0 {
		hr = float64(a.winHits) / float64(winLen)
	}
	errKinds := make(map[string]int64, len(a.errKinds))
	for k, v := range a.errKinds {
		errKinds[k] = v
	}
	var avgLat float64
	if c := a.latencyCount.Load(); c > 0 {
		avgLat = float64(a.totalLatencyMs.Load()) / float64(c)
	}
	return Snapshot{
		TotalReqs:       a.totalReqs.Load(),
		Success:         a.success.Load(),
		Failed:          a.failed.Load(),
		UniqueIPs:       int64(len(a.ips)),
		UniqueCountries: int64(len(a.countries)),
		HitRateWindow:   hr,
		AvgLatencyMs:    avgLat,
		ErrKinds:        errKinds,
	}
}

func (a *Aggregator) Reload(ips map[string]int64, countries map[string]int64, ipCountry map[string]string, total, success, failed int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.ips = ips
	a.countries = countries
	if ipCountry != nil {
		a.ipCountry = ipCountry
	}
	a.totalReqs.Store(total)
	a.success.Store(success)
	a.failed.Store(failed)
}

type CountryStat struct {
	Country string `json:"country"`
	Count   int64  `json:"count"`
}

type IPTop struct {
	IP    string `json:"ip"`
	Count int64  `json:"count"`
}

// DetailedSnapshot 包含完整报表所需数据,直接从内存生成不走 SQL
type DetailedSnapshot struct {
	Snapshot
	Countries  []CountryStat    `json:"countries"`
	TopIPs     []IPTop          `json:"top_ips"`
	MaxDupCnt  int64            `json:"max_dup_count"`
	AvgPerIP   float64          `json:"avg_per_ip"`
	DupRate    float64          `json:"dup_rate"`
	ErrKinds   map[string]int64 `json:"err_kinds_detailed"`
}

func (a *Aggregator) Detailed(topN int) DetailedSnapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	snap := a.snapshotLocked()
	countries := make([]CountryStat, 0, len(a.countries))
	for c, n := range a.countries {
		countries = append(countries, CountryStat{Country: c, Count: n})
	}
	sortByCountDesc(countries)

	tops := make([]IPTop, 0, len(a.ips))
	var maxDup int64
	for ip, n := range a.ips {
		tops = append(tops, IPTop{IP: ip, Count: n})
		if n > maxDup {
			maxDup = n
		}
	}
	sortIPsByCountDesc(tops)
	if len(tops) > topN {
		tops = tops[:topN]
	}

	var avgPerIP, dupRate float64
	if u := int64(len(a.ips)); u > 0 {
		avgPerIP = float64(snap.Success) / float64(u)
		if snap.Success > 0 {
			dupRate = float64(snap.Success-u) / float64(snap.Success)
		}
	}

	errKindsCopy := make(map[string]int64, len(a.errKinds))
	for k, v := range a.errKinds {
		errKindsCopy[k] = v
	}

	return DetailedSnapshot{
		Snapshot:  snap,
		Countries: countries,
		TopIPs:    tops,
		MaxDupCnt: maxDup,
		AvgPerIP:  avgPerIP,
		DupRate:   dupRate,
		ErrKinds:  errKindsCopy,
	}
}

func (a *Aggregator) snapshotLocked() Snapshot {
	winLen := a.winSize
	if !a.winFull {
		winLen = a.winPos
	}
	var hr float64
	if winLen > 0 {
		hr = float64(a.winHits) / float64(winLen)
	}
	errKinds := make(map[string]int64, len(a.errKinds))
	for k, v := range a.errKinds {
		errKinds[k] = v
	}
	var avgLat float64
	if c := a.latencyCount.Load(); c > 0 {
		avgLat = float64(a.totalLatencyMs.Load()) / float64(c)
	}
	return Snapshot{
		TotalReqs:       a.totalReqs.Load(),
		Success:         a.success.Load(),
		Failed:          a.failed.Load(),
		UniqueIPs:       int64(len(a.ips)),
		UniqueCountries: int64(len(a.countries)),
		HitRateWindow:   hr,
		AvgLatencyMs:    avgLat,
		ErrKinds:        errKinds,
	}
}

func sortByCountDesc(s []CountryStat) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].Count > s[j-1].Count; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func sortIPsByCountDesc(s []IPTop) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j].Count > s[j-1].Count; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
