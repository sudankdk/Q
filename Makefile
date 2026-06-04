run :
	go build -o q cmd/main.go
	./q

test :
	go test -v ./...