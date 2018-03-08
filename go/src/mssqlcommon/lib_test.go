/*
	Copyright 2017 Microsoft Corporation

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in
	all copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

package mssqlcommon

import (
	"fmt"
	"os"
	"testing"
)

func TestImportOcfExitCodes(t *testing.T) {
	// Note the values here are intentionally not the real values of these env vars.
	// In particular OCF_SUCCESS is not set to its real value of 0 to be able to check if it's correctly initialized
	var requiredEnvironmentVariables = map[string]string{
		"OCF_SUCCESS":           "1",
		"OCF_ERR_ARGS":          "2",
		"OCF_ERR_CONFIGURED":    "3",
		"OCF_ERR_GENERIC":       "4",
		"OCF_ERR_PERM":          "5",
		"OCF_ERR_UNIMPLEMENTED": "6",
		"OCF_FAILED_MASTER":     "7",
		"OCF_NOT_RUNNING":       "8",
		"OCF_RUNNING_MASTER":    "9",
	}

	// All vars should be 0 initially
	if OCF_SUCCESS != 0 {
		t.Fatal("OCF_SUCCESS is not 0. Test is not starting from clean slate.")
	}

	for key, value := range requiredEnvironmentVariables {
		os.Setenv(key, value)
	}

	// All vars set to valid values
	err := ImportOcfExitCodes()
	if err != nil {
		t.Fatalf("Expected ImportOcfExitCodes to succeed but it failed: %s", err)
	}

	// One var not set
	os.Unsetenv("OCF_SUCCESS")
	err = ImportOcfExitCodes()
	if err == nil {
		t.Fatal("Expected ImportOcfExitCodes to fail but it succeeded")
	}
	if err.Error() != "OCF_SUCCESS is set to an invalid value []" {
		t.Fatalf("ImportOcfExitCodes did not fail with an error about OCF_SUCCESS being unset: %s", err.Error())
	}

	// One var set to invalid value
	os.Setenv("OCF_SUCCESS", "A")
	err = ImportOcfExitCodes()
	if err == nil {
		t.Fatal("Expected ImportOcfExitCodes to fail but it succeeded")
	}
	if err.Error() != "OCF_SUCCESS is set to an invalid value [A]" {
		t.Fatalf("ImportOcfExitCodes did not fail with an error about OCF_SUCCESS being set to A: %s", err.Error())
	}
}

func TestDiagnose(t *testing.T) {
	t.Parallel()

	for _, system := range []bool{true, false} {
		for _, resource := range []bool{true, false} {
			for _, queryProcessing := range []bool{true, false} {
				// Local copies of loop variables for the closure to capture
				system := system
				resource := resource
				queryProcessing := queryProcessing

				t.Run(fmt.Sprintf("system = %t, resource = %t, queryProcessing = %t", system, resource, queryProcessing), func(t *testing.T) {
					t.Parallel()

					diagnostics := Diagnostics{System: system, Resource: resource, QueryProcessing: queryProcessing}
					err := Diagnose(diagnostics)

					if system && resource && queryProcessing {
						if err != nil {
							t.Fatalf("Expected Diagnose to succeed but it failed: %s", err)
						}
					} else {
						if err == nil {
							t.Fatal("Expected Diagnose to fail but it succeeded")
						}

						switch serverUnhealthyError := err.(type) {
						case *ServerUnhealthyError:
							if !system {
								if serverUnhealthyError.RawValue != ServerCriticalError {
									t.Fatalf("Diagnose did not fail with ServerCriticalError: %d", serverUnhealthyError.RawValue)
								}

								if serverUnhealthyError.Inner.Error() != "sp_server_diagnostics result indicates system error" {
									t.Fatalf("Diagnose did not fail with an error about system error: %s", serverUnhealthyError.Inner.Error())
								}
							} else if !resource {
								if serverUnhealthyError.RawValue != ServerModerateError {
									t.Fatalf("Diagnose did not fail with ServerModerateError: %d", serverUnhealthyError.RawValue)
								}

								if serverUnhealthyError.Inner.Error() != "sp_server_diagnostics result indicates resource error" {
									t.Fatalf("Diagnose did not fail with an error about resource error: %s", serverUnhealthyError.Inner.Error())
								}
							} else if !queryProcessing {
								if serverUnhealthyError.RawValue != ServerAnyQualifiedError {
									t.Fatalf("Diagnose did not fail with ServerAnyQualifiedError: %d", serverUnhealthyError.RawValue)
								}

								if serverUnhealthyError.Inner.Error() != "sp_server_diagnostics result indicates query processing error" {
									t.Fatalf("Diagnose did not fail with an error about query processing error: %s", serverUnhealthyError.Inner.Error())
								}
							} else {
								t.Fatal("Unreachable")
							}

						default:
							t.Fatal("Diagnose did not return an error of type ServerUnhealthyError")
						}
					}
				})
			}
		}
	}
}
