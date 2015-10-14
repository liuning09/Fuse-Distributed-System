.PHONY: all rall fmt tags lc doc \
	test-lab1 test-lab2 test-lab3 testv-lab1 testv-lab2 testv-lab3 \
	turnin-zip turnin-lab1 turnin-lab2 turnin-lab3

all:
	go install ./... trib/...

rall:
	go build -a ./... trib/...

fmt:
	gofmt -s -w -l .

tags:
	gotags `find . -name "*.go"` > tags

test-lab1:
	TRIB_LAB=lab1 go test ./...

testv-lab1:
	TRIB_LAB=lab1 go test -v ./...

test-lab2:
	TRIB_LAB=lab2 go test ./...

testv-lab2:
	TRIB_LAB=lab2 go test -v ./...

test-lab3:
	TRIB_LAB=lab3 go test ./...

testv-lab3:
	TRIB_LAB=lab3 go test -v ./...

lc:
	wc -l `find . -name "*.go"`

doc:
	godoc -http=:8000

turnin-zip:
	@ echo "=== Zipping committed files..."
	git archive -o turnin.zip HEAD
	chmod 600 turnin.zip

turnin-lab1: turnin-zip
	@ echo "=== Turning in Lab 1 for `whoami`"
	cp turnin.zip /class/labs/turnin/lab1/`whoami`.zip

turnin-lab2: turnin-zip
	@ echo "=== Turning in Lab 2 for `whoami`"
	cp turnin.zip /class/labs/turnin/lab2/`whoami`.zip

turnin-lab3: turnin-zip
	@ echo "=== Turning in Lab 3 for `whoami`"
	cp turnin.zip /class/labs/turnin/lab3/`whoami`.zip

