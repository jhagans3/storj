// Copyright (C) 2018 Storj Labs, Inc.
// See LICENSE for copying information.

package repeat

import (
	"fmt"
)

// Repeat is a tail recursive function that invokes jobs
func Repeat(jobs []Repeater, errors []string) []string {
	// termination condition
	if len(jobs) <= 0 {
		return errors
	}
	var resJobs []Repeater

	// the "loop"
	for i, job := range jobs {
		repeat, err := job.Repeat()
		if err != nil {
			e := fmt.Sprintf("error at index %v for job %v with state %v err %v", i, job.Name(), job.State(), err)
			errors = append(errors, e)
		}
		if repeat {
			resJobs = append(resJobs, job)
		}
	}

	return Repeat(resJobs, errors)
}
