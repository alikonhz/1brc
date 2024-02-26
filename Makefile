.PHONY run:
run:
	go run main.go ..\..\1brc\measurements.txt

.PHONY bench:
bench:
	go test -bench=. -benchmem -memprofile mem.out -cpuprofile cpu.out
