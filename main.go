package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sort"
	"time"
)

type Measurement struct {
	Name string
	Min  int16
	Max  int16

	Hash  uint32
	sum   int32
	count int
}

const (
	// code of zero in ASCII table
	zeroCode = 48
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

	fmt.Fprint(resF, "{")
	comma := ""
	for i := 0; i < len(res); i++ {
		avg := float32(res[i].sum) / float32(res[i].count)
		fmt.Fprintf(resF, "%s%s=%.1f/%.1f/%.1f", comma, res[i].Name, float32(res[i].Min)/10.0, float32(avg)/10.0, float32(res[i].Max)/10.0)
		comma = ", "
	}

	fmt.Fprint(resF, "}")

	end := time.Now()
	d := end.Sub(start)
	fmt.Printf("processed in %d ms", d.Milliseconds())
}

func parseFloat(input []byte) (int16, error) {
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
		return -f, nil
	}

	return f, nil
}

func measure(f *os.File) ([]*Measurement, error) {

	const prefixBufLen = 64
	buffer := make([]byte, (1024*1024*1024)+prefixBufLen) // 1GB + 64 bytes

	var result []*Measurement
	allCities := make([][]*Measurement, 65535)

	// will become true when we reach EOF
	end := false

	var (
		cityStart int
		cityEnd   int
		tempStart int
		tempEnd   int
	)

	const (
		cr        = '\r'
		lf        = '\n'
		semicolon = ';'
	)

	crc := crc32.New(crc32.MakeTable(crc32.Koopman))

	addCity := func() error {
		city := buffer[cityStart:cityEnd]
		_, err := crc.Write(city)
		if err != nil {
			return err
		}
		val, err := parseFloat(buffer[tempStart:tempEnd])
		if err != nil {
			return err
		}

		crcVal := crc.Sum32()
		cityHash := crcVal % 65535
		crc.Reset()

		mr := allCities[cityHash]
		var cityMr *Measurement
		if len(mr) == 0 {
			mr = make([]*Measurement, 5)
			cityMr = &Measurement{
				Name:  string(city),
				Min:   99,
				Max:   -99,
				Hash:  crcVal,
				sum:   0,
				count: 0,
			}
			result = append(result, cityMr)
			mr[0] = cityMr
			allCities[cityHash] = mr
		} else {
			for i := 0; i < len(mr); i++ {
				cityMr = mr[i]
				if cityMr != nil && cityMr.Hash == crcVal {
					break
				}

				if cityMr == nil {
					cityMr = &Measurement{
						Name:  string(city),
						Min:   99,
						Max:   -99,
						Hash:  crcVal,
						sum:   0,
						count: 0,
					}
					result = append(result, cityMr)
					mr[i] = cityMr
					break
				}
			}
		}

		if cityMr == nil {
			panic(fmt.Errorf("%+v", mr))
		}

		cityMr.add(val)

		return nil
	}

	prefixIndex := prefixBufLen
	for !end {
		read, err := f.Read(buffer[prefixBufLen:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				end = true
			} else {
				return nil, err
			}
		}

		lfIndex := prefixBufLen + read - 1
		if !end {
			for buffer[lfIndex] != lf {
				lfIndex--
			}
		}

		// we always start from city
		cityStart = prefixIndex

		for i := prefixIndex; i < lfIndex; i++ {
			// check every byte
			switch buffer[i] {
			case cr:
			case lf:
				tempEnd = i
				err = addCity()
				if err != nil {
					return nil, err
				}
				cityStart = i + 1
			case semicolon:
				cityEnd = i
				tempStart = i + 1
			}
		}

		tempEnd = lfIndex
		if cityStart < lfIndex {
			// we have last unprocessed city
			err = addCity()
			if err != nil {
				return nil, err
			}
		}

		prefixIndex = prefixBufLen
		for i := prefixBufLen + read - 1; i > lfIndex; i-- {
			prefixIndex--
			buffer[prefixIndex] = buffer[i]
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result, nil
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
