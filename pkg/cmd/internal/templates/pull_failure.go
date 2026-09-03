package templates

import "fmt"

// SchemaPullFailure describes a schema read that the server refused or that
// never reached it, as the CLI reports it to the operator.
type SchemaPullFailure struct {
	// Operation names the command that failed, so the same rendering serves
	// every command that reads live schema.
	Operation   string
	Database    string
	Environment string

	// Status and ErrorCode come from the API's response and are omitted when
	// the request never got one, which is how a refusal by the server reads
	// differently from a failure to reach it.
	Status    int
	ErrorCode string

	// Message is the error as the client resolved it, which for an API error
	// is the server's own message.
	Message string
}

// WriteSchemaPullFailure renders a failed schema read. The database and
// environment are echoed back because an operator running the same command
// against several targets needs to know which one refused, and the API status
// and error code are shown separately from the message so a machine-readable
// code an operator can look up is not buried in prose.
func WriteSchemaPullFailure(failure SchemaPullFailure) {
	fmt.Printf("%s%s failed%s\n", ANSIRed, failure.Operation, ANSIReset)
	fmt.Printf("  Database: %s\n", failure.Database)
	fmt.Printf("  Environment: %s\n", failure.Environment)
	if failure.Status != 0 {
		fmt.Printf("  API status: HTTP %d\n", failure.Status)
		if failure.ErrorCode != "" {
			fmt.Printf("  Error code: %s\n", failure.ErrorCode)
		}
	}
	fmt.Printf("  Error: %s\n", failure.Message)
}
