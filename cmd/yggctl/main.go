package main

import (
	"fmt"
	"os"

	"git.sr.ht/~spc/go-log"

	"github.com/redhatinsights/yggdrasil/internal/constants"
	"github.com/urfave/cli/v2"
)

func main() {
	app := cli.NewApp()
	app.Name = "yggctl"
	app.Version = constants.Version
	app.Usage = "control and interact with yggd"

	app.Flags = []cli.Flag{
		&cli.BoolFlag{
			Name:   "generate-man-page",
			Hidden: true,
		},
		&cli.BoolFlag{
			Name:   "generate-markdown",
			Hidden: true,
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:   "generate",
			Usage:  `Generate messages for publishing to client "in" topics.`,
			Hidden: true,
			Subcommands: []*cli.Command{
				{
					Name:    "data-message",
					Usage:   "Generate a data message.",
					Aliases: []string{"data"},
					Flags: []cli.Flag{
						&cli.IntFlag{
							Name:    "version",
							Aliases: []string{"v"},
							Value:   1,
							Usage:   "set version to `NUM`",
						},
						&cli.StringFlag{
							Name:    "response-to",
							Aliases: []string{"r"},
							Usage:   "reply to message `UUID`",
						},
						&cli.StringFlag{
							Name:    "metadata",
							Aliases: []string{"m"},
							Value:   "{}",
							Usage:   "set metadata to `JSON`",
						},
						&cli.StringFlag{
							Name:     "directive",
							Aliases:  []string{"d"},
							Required: true,
							Usage:    "set directive to `STRING`",
						},
					},
					Action: generateDataMessageAction,
				},
				{
					Name:    "control-message",
					Usage:   "Generate a control message.",
					Aliases: []string{"control"},
					Flags: []cli.Flag{
						&cli.IntFlag{
							Name:    "version",
							Aliases: []string{"v"},
							Value:   1,
							Usage:   "set version to `NUM`",
						},
						&cli.StringFlag{
							Name:    "response-to",
							Aliases: []string{"r"},
							Usage:   "reply to message `UUID`",
						},
						&cli.StringFlag{
							Name:     "type",
							Aliases:  []string{"t"},
							Required: true,
							Usage:    "set message type to `STRING`",
						},
					},
					Action: generateControlMessageAction,
				},
			},
		},
		{
			Name:  "workers",
			Usage: "Interact with yggdrasil workers",
			Subcommands: []*cli.Command{
				{
					Name:        "list",
					Usage:       "List currently connected workers",
					Description: `The list command prints a list of currently connected workers, along with the workers "features" table.`,
					Flags: []cli.Flag{
						&cli.StringFlag{
							Name:  "format",
							Usage: "Print output in `FORMAT` (json, table or text)",
							Value: "text",
						},
					},
					Action: workersAction,
				},
			},
		},
		{
			Name:        "dispatch",
			Usage:       "Dispatch data to a worker locally",
			UsageText:   "yggctl dispatch [command options] FILE",
			Description: "The dispatch command reads FILE and sends its content to a yggdrasil worker running locally. If FILE is -, content is read from stdin.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "worker",
					Aliases:  []string{"w"},
					Usage:    "Send data to `WORKER`",
					Required: true,
				},
				&cli.StringFlag{
					Name:    "metadata",
					Aliases: []string{"m"},
					Usage:   "Attach `JSON` as metadata to the message",
					Value:   "{}",
				},
			},
			Action: dispatchAction,
		},
		{
			Name:        "message-journal",
			Usage:       "Display a list of all worker messages and emitted events",
			UsageText:   "yggctl message-journal",
			Description: "The message-journal command sends a dbus call to yggdrasil to retrieve a message journal containing all worker messages and emitted events.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "truncate-message",
					Aliases:  []string{"t"},
					Usage:    "Truncates worker event messages if they exceed the specified character count (10 by default).",
					Required: false,
				},
				&cli.StringFlag{
					Name:     "worker",
					Aliases:  []string{"w"},
					Usage:    "Only display worker messages and emitted events for the specified worker.",
					Required: false,
				},
				&cli.StringFlag{
					Name:     "message-id",
					Aliases:  []string{"m"},
					Usage:    "Only display worker messages and emitted events for the entries with the specified message id.",
					Required: false,
				},
			},
			Action: func(ctx *cli.Context) error {
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
			},
		},
		{
			Name:        "listen",
			Usage:       "Listen to worker event output",
			Description: "The listen command waits for events emitted by the specified worker and prints them to the console.",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:     "worker",
					Aliases:  []string{"w"},
					Usage:    "Listen for events emitted by `WORKER`",
					Required: true,
				},
			},
			Action: listenAction,
		},
	}

	app.Action = generateManPage
	app.EnableBashCompletion = true

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func generateManPage(c *cli.Context) error {
	if c.Bool("generate-man-page") || c.Bool("generate-markdown") {
		type GenerationFunc func() (string, error)
		var generationFunc GenerationFunc
		if c.Bool("generate-man-page") {
			generationFunc = c.App.ToMan
		} else if c.Bool("generate-markdown") {
			generationFunc = c.App.ToMarkdown
		}
		data, err := generationFunc()
		if err != nil {
			return err
		}
		fmt.Println(data)
		return nil
	}

	return cli.ShowAppHelp(c)
}
