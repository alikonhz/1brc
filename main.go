package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"
)

type Measurement struct {
	Name string
	Min  float32
	Max  float32

	sum   float32
	count int
}

const (
	dec1 float32 = 0.1
	one  float32 = 1.0
	ten  float32 = 10.0

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
		fmt.Fprintf(resF, "%s%s=%.1f/%.1f/%.1f", comma, res[i].Name, res[i].Min, res[i].sum/float32(res[i].count), res[i].Max)
		comma = ", "
	}

	fmt.Fprint(resF, "}")

	end := time.Now()
	d := end.Sub(start)
	fmt.Printf("processed in %d ms", d.Milliseconds())
}

func parseFloat(input []byte) (float32, error) {
	s := input
	var f float32 = 0.0
	minus := false
	if s[0] == '-' {
		minus = true
		s = s[1:]
	}
	f += dec1 * float32(s[len(s)-1]-zeroCode)
	// minus last symbol and dot
	s = s[:len(s)-2]
	f += one * float32(s[len(s)-1]-zeroCode)
	s = s[:len(s)-1]
	if len(s) > 0 {
		f += ten * float32(s[0]-zeroCode)
	}
	if minus {
		return -f, nil
	}

	return f, nil
}

func measure(f *os.File) ([]*Measurement, error) {

	buffer := make([]byte, 1024*1024*1024) // 1GB

	var keys []string
	m := make(map[string]*Measurement)

	// will become true when we reach EOF
	end := false

	// we will copy data from the main buffer here
	cityBuffer := make([]byte, 128) // city buffer - we don't expect cities with more than 128 bytes
	tempBuffer := make([]byte, 8)   // temperature buffer

	var (
		cityIndex int
		tempIndex int
		isCity    bool = true
	)

	const (
		cr = '\r'
		lf = '\n'
		sc = ';'
	)

	addCity := func() error {
		city := string(cityBuffer[0:cityIndex])
		val, err := parseFloat(tempBuffer[0:tempIndex])
		if err != nil {
			return err
		}

		mr, ok := m[city]
		if !ok {
			m[city] = &Measurement{
				Name:  city,
				count: 0,
				sum:   0,
				Min:   100,
				Max:   -100,
			}
			keys = append(keys, city)
		} else {
			mr.add(float32(val))
		}

		return nil
	}

	for !end {
		read, err := f.Read(buffer)
		if err != nil {
			if errors.Is(err, io.EOF) {
				end = true
			} else {
				return nil, err
			}
		}

		for i := 0; i < read; i++ {
			// check every byte
			switch buffer[i] {
			case cr:
			case lf:
				if cityIndex > 0 && tempIndex > 0 {
					err := addCity()
					if err != nil {
						return nil, err
					}

					cityIndex = 0
					tempIndex = 0
					isCity = true
				}
			case sc: // semicolon
				isCity = false
				tempIndex = 0
			default:
				if isCity {
					cityBuffer[cityIndex] = buffer[i]
					cityIndex++
				} else {
					tempBuffer[tempIndex] = buffer[i]
					tempIndex++
				}
			}
		}
	}

	sort.Strings(keys)

	res := make([]*Measurement, len(keys), len(keys))
	for i := 0; i < len(keys); i++ {
		res[i] = m[keys[i]]
	}

	return res, nil
}

func (m *Measurement) add(val float32) {
	m.sum += val
	m.count++
	if val < m.Min {
		m.Min = val
	} else if val > m.Max {
		m.Max = val
	}
}
