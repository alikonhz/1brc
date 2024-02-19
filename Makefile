.PHONY run:
run:
	# relative path - on another computer can be different
	# hence be careful
	go run main.go ..\..\1brc\measurements.txt

.PHONY bench:
bench:
	go test -bench=. -benchmem -memprofile mem.out -cpuprofile cpu.out
