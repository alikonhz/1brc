package main

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkMeasure(b *testing.B) {
	f, err := os.Open("d:\\github\\alikonhz\\1brc\\measurements.txt")
	if err != nil {
		panic(err)
	}

	defer f.Close()

	res, err := measure(f)
	if err != nil {
		panic(err)
	}

	resF, err := os.Create("res.txt")
	if err != nil {
		panic(err)
	}

	defer resF.Close()

	fmt.Fprint(resF, "{")
	comma := ""
	for i := 0; i < len(res); i++ {
		avg := float32(res[i].sum) / float32(res[i].count)
		fmt.Fprintf(resF, "%s%s=%.1f/%.1f/%.1f", comma, res[i].Name, float32(res[i].Min)/10.0, float32(avg)/10.0, float32(res[i].Max)/10.0)
		comma = ", "
	}

	fmt.Fprint(resF, "}")
}
