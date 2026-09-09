// Package docdemo holds the reproducible, fictional scenarios used by the CLI guide.
// The executable generator is excluded from normal builds; go generate runs it
// explicitly against the current production templates.
//
//go:generate sh -c "go run main.go > ../../../assets/src/cli-demo.json"
package docdemo
