package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	// code of zero in ASCII table
	zeroCode              = 48
	CR                    = '\r'
	LF                    = '\n'
	semicolon             = ';'
	mSize          uint64 = 32768
	fnvOffsetBasis uint64 = 0xcbf29ce484222325
	fnvPrime       uint64 = 0x100000001b3
)

func main() {
	if len(os.Args) < 2 {
		panic("please pass path to the measurements.txt file")
	}

	start := time.Now()

	fName := os.Args[1]
	f, err := os.Open(fName)
	if err != nil {
		panic(err)
	}

	resF, err := os.Create("res.txt")
	if err != nil {
		panic(err)
	}

	defer f.Close()
	defer resF.Close()

	res, err := measure(f)
	if err != nil {
		panic(err)
	}

	fmt.Fprint(resF, "{\n")
	comma := ""
	for i := 0; i < len(res); i++ {
		avg := float32(res[i].sum) / float32(res[i].count)
		fmt.Fprintf(resF, "%s%s=%.1f/%.1f/%.1f\n", comma, res[i].Name, float32(res[i].Min)/10.0, float32(avg)/10.0, float32(res[i].Max)/10.0)
		//comma = ", "
		comma = " "
	}

	fmt.Fprint(resF, "}\n")

	end := time.Now()
	d := end.Sub(start)
	fmt.Printf("processed in %d ms", d.Milliseconds())
}

func parseFloat(input []byte) int16 {
	s := input
	var f int16 = 0
	minus := false
	if s[0] == '-' {
		minus = true
		s = s[1:]
	}
	f += int16(s[len(s)-1] - zeroCode)
	// minus last symbol and dot
	s = s[:len(s)-2]
	f += 10 * int16(s[len(s)-1]-zeroCode)
	s = s[:len(s)-1]
	if len(s) > 0 {
		f += 100 * int16(s[0]-zeroCode)
	}
	if minus {
		return -f
	}

	return f
}

type worker struct {
	allCities [][]*Measurement
	indexes   []uint64
	//crc       hash.Hash64
}

func newWorker() *worker {
	return &worker{
		allCities: make([][]*Measurement, mSize),
	}
}

func (w *worker) process(buffer []byte, wg *sync.WaitGroup) {
	var (
		cityStart int
		cityEnd   int
		tempStart int
		tempEnd   int
	)

	// we always start from city
	cityStart = 0
	bufLen := len(buffer)
	for i := 0; i < bufLen; i++ {
		// check every byte
		switch buffer[i] {
		case CR:
		case LF:
			tempEnd = i
			w.addCity(buffer, cityStart, cityEnd, tempStart, tempEnd)
			cityStart = i + 1
		case semicolon:
			cityEnd = i
			tempStart = i + 1
		}
	}

	// we have last unprocessed city
	w.addCity(buffer, cityStart, cityEnd, tempStart, bufLen)

	wg.Done()
}

func (w *worker) addCity(buffer []byte, cityStart, cityEnd, tempStart, tempEnd int) {
	city := buffer[cityStart:cityEnd]

	val := parseFloat(buffer[tempStart:tempEnd])

	crcVal := fnvOffsetBasis
	for i := 0; i < len(city); i++ {
		crcVal ^= uint64(city[i])
		crcVal *= fnvPrime
	}

	cityIndex := crcVal % mSize
	mr := w.allCities[cityIndex]
	var cityMr *Measurement
	if len(mr) == 0 {
		mr = make([]*Measurement, 2)
		cityMr = &Measurement{
			Name:  string(city),
			Min:   99,
			Max:   -99,
			Hash:  crcVal,
			sum:   0,
			count: 0,
		}
		w.indexes = append(w.indexes, cityIndex)
		mr[0] = cityMr
		w.allCities[cityIndex] = mr
	} else {
		if mr[0].Hash == crcVal {
			cityMr = mr[0]
		} else {
			cityMr = mr[1]
			if cityMr == nil {
				cityMr = &Measurement{
					Name:  string(city),
					Min:   99,
					Max:   -99,
					Hash:  crcVal,
					sum:   0,
					count: 0,
				}

				mr[1] = cityMr
			}
		}
	}

	if cityMr == nil {
		panic(fmt.Errorf("%+v", mr))
	}

	cityMr.add(val)
}

type mergeRequest struct {
	measurements [][]*Measurement
	indexes      []uint64
}

type merger struct {
	m       map[uint64]*Measurement
	results []*Measurement
	mergeCh chan mergeRequest

	resChan chan []*Measurement
}

func newMerger() *merger {
	return &merger{
		m:       make(map[uint64]*Measurement),
		mergeCh: make(chan mergeRequest, 8),
		resChan: make(chan []*Measurement),
	}
}

func (m *merger) finish() []*Measurement {
	close(m.mergeCh)

	r := <-m.resChan

	return r
}

func (m *merger) addMerge(request mergeRequest) {
	m.mergeCh <- request
}

func (m *merger) merge() {
	for request := range m.mergeCh {
		for _, index := range request.indexes {
			for _, mr := range request.measurements[index] {
				if mr == nil {
					break
				}
				res, ok := m.m[mr.Hash]
				if !ok {
					m.m[mr.Hash] = mr
					m.results = append(m.results, mr)
				} else {
					res.merge(mr)
				}
			}
		}
	}

	sort.Slice(m.results, func(i, j int) bool {
		return m.results[i].Name < m.results[j].Name
	})

	m.resChan <- m.results
}

func measure(f *os.File) ([]*Measurement, error) {

	// 64 bytes for temp buffer
	// 1GB as a full buffer
	const prefixBufLen = 64
	buffer := make([]byte, prefixBufLen+(1024*1024*1024))

	// will become true when we reach EOF
	end := false

	// on the first read we always start at 64 (i.e. where the 1GB buffer starts)
	prefixIndex := prefixBufLen

	mg := newMerger()
	go mg.merge()

	for !end {
		// put data from the file into the buffer starting at index 64
		read, err := f.Read(buffer[prefixBufLen:])

		// assume LF is the last read element in the buffer
		lfIndex := prefixBufLen + read - 1

		if err != nil {
			if read == 0 {
				// we're done reading
				break
			}
			if errors.Is(err, io.EOF) {
				end = true
				// if we reached EOF -> change lfIndex to the last element in the buffer
				lfIndex = len(buffer) - 1
			} else {
				return nil, err
			}
		}

		if !end {
			// if we haven't reached the EOF yet ->
			// decrease lfIndex while last element is not LF
			for buffer[lfIndex] != LF {
				lfIndex--
			}
		}

		workBuffer := buffer[prefixIndex:lfIndex]
		workers := make([]*worker, runtime.NumCPU())

		// start index
		si := 0

		batchSize := len(workBuffer) / len(workers)

		var wg sync.WaitGroup
		wg.Add(len(workers))

		for i := 0; i < len(workers); i++ {
			w := newWorker()
			workers[i] = w

			// end index
			ei := padToLF(workBuffer, si, batchSize)
			go w.process(workBuffer[si:ei], &wg)
			si = ei + 1
		}

		wg.Wait()

		for _, w := range workers {
			rq := mergeRequest{
				measurements: w.allCities,
				indexes:      w.indexes,
			}
			mg.addMerge(rq)
		}

		prefixIndex = prefixBufLen
		for i := prefixBufLen + read - 1; i > lfIndex; i-- {
			prefixIndex--
			buffer[prefixIndex] = buffer[i]
		}
	}

	r := mg.finish()
	return r, nil
}

func padToLF(buffer []byte, start int, batchSize int) int {
	newEnd := start + batchSize
	if len(buffer) < newEnd {
		return len(buffer)
	}

	// shift end index to the right until we meet LF
	for buffer[newEnd] != LF {
		newEnd++
	}

	return newEnd
}

type Measurement struct {
	Name string
	Min  int16
	Max  int16

	Hash  uint64
	sum   int32
	count int
}

func (m *Measurement) add(val int16) {
	m.sum += int32(val)
	m.count++
	if val < m.Min {
		m.Min = val
	} else if val > m.Max {
		m.Max = val
	}
}

func (m *Measurement) merge(other *Measurement) {
	m.sum += other.sum
	m.count += other.count
	m.Min = min(m.Min, other.Min)
	m.Max = max(m.Max, other.Max)
}
