package cli

import (
	"fmt"
	"os"
)

// getEnvStr retrieves an env-passed string parameter named str. Panics if the named envvar is unset.
func GetEnvStr(str string) string {
	strVal, ok := os.LookupEnv(str)

	if !ok {
		panic(fmt.Sprintf("%s not specified", str))
	}

	return strVal
}
