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
				&cli.BoolFlag{
					Name:     "persistent",
					Aliases:  []string{"p"},
					Usage:    "Display worker messages and emitted events gathered across multiple sessions from persistent storage.",
					Required: false,
				},
				&cli.UintFlag{
					Name:     "truncate-message",
					Aliases:  []string{"t"},
					Value:    15,
					Usage:    "Truncates worker event messages if they exceed the specified character count.",
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
				&cli.StringFlag{
					Name:     "from-time",
					Aliases:  []string{"ft"},
					Usage:    "Only display worker messages and emitted events sent starting from the timestamp provided.",
					Required: false,
				},
				&cli.StringFlag{
					Name:     "to-time",
					Aliases:  []string{"tt"},
					Usage:    "Only display worker messages and emitted events sent up to the timestamp provided.",
					Required: false,
				},
			},
			Action: messageJournalAction,
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
