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
		fmt.Fprintf(resF, "%s%s=%.1f/%.1f/%.1f", comma, res[i].Name, res[i].Min, res[i].sum/float32(res[i].count), res[i].Max)
		comma = ", "
	}

	fmt.Fprint(resF, "}")
}
