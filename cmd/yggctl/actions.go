package main

import (
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/godbus/dbus/v5"
	"github.com/urfave/cli/v2"
)

func MessageJournalAction(ctx *cli.Context) error {
	var conn *dbus.Conn
	var err error

	if os.Getenv("DBUS_SESSION_BUS_ADDRESS") != "" {
		conn, err = dbus.ConnectSessionBus()
	} else {
		conn, err = dbus.ConnectSystemBus()
	}
	if err != nil {
		return cli.Exit(fmt.Errorf("cannot connect to bus: %w", err), 1)
	}

	var journalEntries []map[string]string
	obj := conn.Object("com.redhat.Yggdrasil1", "/com/redhat/Yggdrasil1")
	if err := obj.Call("com.redhat.Yggdrasil1.MessageJournal", dbus.Flags(0)).Store(&journalEntries); err != nil {
		return cli.Exit(fmt.Errorf("cannot list messages: %v", err), 1)
	}

	// Set the worker message truncate length from the user-provided arguments
	// Default to 10 if not provided or invalid value provided.
	truncateLength := ctx.Int("truncate-message")
	if truncateLength <= 0 {
		truncateLength = 10
	}
	// Get the user provided 'worker'/'message-id' arguments to filter journal entries if provided.
	selectedWorker := ctx.String("worker")
	selectedMessageID := ctx.String("message-id")

	// Filter the journal entries by the user provided arguments.
	var filteredJournalEntries []string
	for idx, entry := range journalEntries {
		// If only a specific worker is selected, skip all journal entries from other workers.
		if len(selectedWorker) != 0 {
			if entry["worker_name"] != selectedWorker {
				continue
			}
		}
		// If only a specific message id is selected, skip all journal entries with other message ids.
		if len(selectedMessageID) != 0 {
			if entry["message_id"] != selectedMessageID {
				continue
			}
		}
		// Truncate the worker messages by the truncate length specified.
		messageMaxSize := len(entry["worker_message"])
		workerMessage := entry["worker_message"]
		if messageMaxSize >= truncateLength {
			messageMaxSize = truncateLength
			workerMessage = fmt.Sprintf("%+v...", entry["worker_message"][:messageMaxSize])
		}

		filteredJournalEntries = append(filteredJournalEntries, fmt.Sprintf("%d\t%s\t%s\t%s\t%s\t%v\t%s\n", idx, entry["message_id"], entry["sent"], entry["worker_name"], entry["response_to"], entry["worker_event"], workerMessage))
	}
	if len(filteredJournalEntries) == 0 {
		fmt.Println("No journal entries found.")
		return nil
	}

	writer := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
	fmt.Fprint(writer, "MESSAGE #\tMESSAGE ID\tSENT\tWORKER NAME\tRESPONSE TO\tWORKER EVENT\tWORKER MESSAGE\n")
	for _, filteredEntry := range filteredJournalEntries {
		fmt.Fprint(writer, filteredEntry)
	}
	writer.Flush()

	return nil
}
