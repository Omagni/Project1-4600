package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	// for sorting the processes based on duration/priority
	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	SJFSchedule(os.Stdout, "Shortest-job-first", processes)
	//
	SJFPrioritySchedule(os.Stdout, "Priority", processes)
	//
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {

	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// Sort the processes based on priority
	priorityProcesses := make([]Process, len(processes))
	copy(priorityProcesses, processes)
	sort.Slice(priorityProcesses, func(i, j int) bool {
		return priorityProcesses[i].Priority < priorityProcesses[j].Priority
	})

	for i := range priorityProcesses {
		if priorityProcesses[i].ArrivalTime > 0 {
			waitingTime = serviceTime - priorityProcesses[i].ArrivalTime
			if waitingTime < 0 {
				waitingTime = 0
			}
		}
		totalWait += float64(waitingTime)

		start := waitingTime + priorityProcesses[i].ArrivalTime

		turnaround := priorityProcesses[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := priorityProcesses[i].BurstDuration + priorityProcesses[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += priorityProcesses[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   priorityProcesses[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

func SJFSchedule(w io.Writer, title string, processes []Process) {

	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// Sort the processes based on burst duration
	burstProcesses := make([]Process, len(processes))
	copy(burstProcesses, processes)
	sort.Slice(burstProcesses, func(i, j int) bool {
		return burstProcesses[i].BurstDuration < burstProcesses[j].BurstDuration
	})

	for i := range burstProcesses {
		if burstProcesses[i].ArrivalTime > 0 {
			waitingTime = serviceTime - burstProcesses[i].ArrivalTime
			if waitingTime < 0 {
				waitingTime = 0
			}
		}
		totalWait += float64(waitingTime)

		start := waitingTime + burstProcesses[i].ArrivalTime

		turnaround := burstProcesses[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := burstProcesses[i].BurstDuration + burstProcesses[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += burstProcesses[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   burstProcesses[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func RRSchedule(w io.Writer, title string, processes []Process) {

	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)

	// copy of processes to not mess with the originals burst times
	copyProcesses := make([]Process, len(processes))
	copy(copyProcesses, processes)

	qtime := 3
	complete := 0 //completed proccesses (when burst = 0)
	current := 0  // selected process
	it := 0       // every iteration

	for complete < len(copyProcesses) {

		// calculations
		if copyProcesses[complete].ArrivalTime > 0 {
			waitingTime = serviceTime - copyProcesses[complete].ArrivalTime
			if waitingTime < 0 {
				waitingTime = 0
			}
		}
		totalWait += float64(waitingTime)
		start := waitingTime + copyProcesses[current].ArrivalTime
		turnaround := copyProcesses[complete].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)
		completion := copyProcesses[complete].BurstDuration + copyProcesses[complete].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		gantt = append(gantt, TimeSlice{
			PID:   copyProcesses[complete].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
		schedule[complete] = []string{
			fmt.Sprint(processes[complete].ProcessID),
			fmt.Sprint(processes[complete].Priority),
			fmt.Sprint(processes[complete].BurstDuration),
			fmt.Sprint(processes[complete].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += copyProcesses[complete].BurstDuration

		// decrement current burst duration by qtime
		copyProcesses[current].BurstDuration = copyProcesses[current].BurstDuration - int64(qtime)
		// do not want burst time to go below 0 so just set to 0 if goes below
		if copyProcesses[complete].BurstDuration < 0 {
			copyProcesses[complete].BurstDuration = 0
		}

		// increment complete because if burst = 0, then the process is complete
		if int(copyProcesses[complete].BurstDuration) <= 0 {
			complete++
		}

		// if every process has 0 burst time
		temp := 0
		for j := 0; j < len(copyProcesses); j++ {
			if copyProcesses[j].BurstDuration == 0 {
				temp++
			}
		}

		if temp == len(processes) {
			break
		} else {
			temp = 0
		}

		// increase current per iteration
		current++
		// force current to reset
		if current == 3 {
			current = 0
		}
		// increate iteration count
		it++
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)

}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
