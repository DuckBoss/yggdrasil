package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"text/tabwriter"
	"text/template"

	"github.com/godbus/dbus/v5"
	"github.com/google/uuid"
	"github.com/redhatinsights/yggdrasil"
	"github.com/redhatinsights/yggdrasil/ipc"
	"github.com/urfave/cli/v2"
)

func generateDataMessageAction(c *cli.Context) error {
	var metadata map[string]string
	if err := json.Unmarshal([]byte(c.String("metadata")), &metadata); err != nil {
		return cli.Exit(fmt.Errorf("cannot unmarshal metadata: %w", err), 1)
	}

	data, err := generateMessage(
		"data",
		c.String("response-to"),
		c.String("directive"),
		c.Args().First(),
		metadata,
		c.Int("version"),
	)
	if err != nil {
		return cli.Exit(fmt.Errorf("cannot marshal message: %w", err), 1)
	}

	fmt.Println(string(data))

	return nil
}

func generateControlMessageAction(c *cli.Context) error {
	data, err := generateMessage(
		c.String("type"),
		c.String("response-to"),
		"",
		c.Args().First(),
		nil,
		c.Int("version"),
	)
	if err != nil {
		return cli.Exit(fmt.Errorf("cannot marshal message: %w", err), 1)
	}

	fmt.Println(string(data))

	return nil
}

func messageJournalAction(ctx *cli.Context) error {
	var conn *dbus.Conn
	var err error

	if os.Getenv("DBUS_SESSION_BUS_ADDRESS") != "" {
		conn, err = dbus.ConnectSessionBus()
		if err != nil {
			return cli.Exit(fmt.Errorf("cannot connect to session bus: %w", err), 1)
		}
	} else {
		conn, err = dbus.ConnectSystemBus()
		if err != nil {
			return cli.Exit(fmt.Errorf("cannot connect to system bus: %w", err), 1)
		}
	}

	var journalEntries []map[string]string
	args := []interface{}{
		ctx.Uint("truncate-message"),
		ctx.String("message-id"),
		ctx.String("worker"),
		ctx.String("since"),
		ctx.String("until"),
		ctx.Bool("persistent"),
	}
	obj := conn.Object("com.redhat.Yggdrasil1", "/com/redhat/Yggdrasil1")
	if err := obj.Call("com.redhat.Yggdrasil1.MessageJournal", dbus.Flags(0), args...).Store(&journalEntries); err != nil {
		return cli.Exit(fmt.Errorf("cannot list message journal entries: %v", err), 1)
	}

	switch ctx.String("format") {
	case "json":
		data, err := json.Marshal(journalEntries)
		if err != nil {
			return cli.Exit(fmt.Errorf("cannot marshal journal entries: %v", err), 1)
		}
		fmt.Println(string(data))
	case "text":
		journalTextTemplate := template.New("journalTextTemplate")
		journalTextTemplate, err := journalTextTemplate.Parse(
			"{{range .}}{{.message_id}} : {{.sent}} : {{.worker_name}} : " +
				"{{if .response_to}}{{.response_to}}{{else}}...{{end}} : " +
				"{{if .worker_event}}{{.worker_event}}{{else}}...{{end}} : " +
				"{{if .worker_message}}{{.worker_message}}{{else}}...{{end}}\n{{end}}",
		)
		if err != nil {
			return fmt.Errorf("cannot parse journal text template parameters: %w", err)
		}
		var compiledTextTemplate bytes.Buffer
		textCompileErr := journalTextTemplate.Execute(&compiledTextTemplate, journalEntries)
		if textCompileErr != nil {
			return fmt.Errorf("cannot compile journal text template: %w", textCompileErr)
		}
		fmt.Println(compiledTextTemplate.String())
	case "table":
		writer := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
		fmt.Fprint(
			writer,
			"MESSAGE #\tMESSAGE ID\tSENT\tWORKER NAME\tRESPONSE TO\tWORKER EVENT\tWORKER MESSAGE\n",
		)
		for idx, entry := range journalEntries {
			fmt.Fprintf(
				writer,
				"%d\t%s\t%s\t%s\t%s\t%v\t%s\n",
				idx,
				entry["message_id"],
				entry["sent"],
				entry["worker_name"],
				entry["response_to"],
				entry["worker_event"],
				entry["worker_message"],
			)
		}
		if err := writer.Flush(); err != nil {
			return cli.Exit(fmt.Errorf("unable to flush tab writer: %v", err), 1)
		}
	default:
		return cli.Exit(fmt.Errorf("unknown format type: %v", ctx.String("format")), 1)
	}

	return nil
}

func workersAction(c *cli.Context) error {
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

	obj := conn.Object("com.redhat.Yggdrasil1", "/com/redhat/Yggdrasil1")
	var workers map[string]map[string]string
	if err := obj.Call("com.redhat.Yggdrasil1.ListWorkers", dbus.Flags(0)).Store(&workers); err != nil {
		return cli.Exit(fmt.Errorf("cannot list workers: %v", err), 1)
	}

	switch c.String("format") {
	case "json":
		data, err := json.Marshal(workers)
		if err != nil {
			return cli.Exit(fmt.Errorf("cannot marshal workers: %v", err), 1)
		}
		fmt.Println(string(data))
	case "table":
		writer := tabwriter.NewWriter(os.Stdout, 4, 4, 2, ' ', 0)
		fmt.Fprintf(writer, "WORKER\tFIELD\tVALUE\n")
		for worker, features := range workers {
			for field, value := range features {
				fmt.Fprintf(writer, "%v\t%v\t%v\n", worker, field, value)
			}
			_ = writer.Flush()
		}
	case "text":
		for worker, features := range workers {
			for field, value := range features {
				fmt.Printf("%v - %v: %v\n", worker, field, value)
			}
		}
	default:
		return cli.Exit(fmt.Errorf("unknown format type: %v", c.String("format")), 1)
	}

	return nil
}

func dispatchAction(c *cli.Context) error {
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

	var metadata map[string]string
	if err := json.Unmarshal([]byte(c.String("metadata")), &metadata); err != nil {
		return cli.Exit(fmt.Errorf("cannot unmarshal metadata: %w", err), 1)
	}

	var data []byte
	var r io.Reader
	if c.Args().First() == "-" {
		r = os.Stdin
	} else {
		r, err = os.Open(c.Args().First())
	}
	if err != nil {
		return cli.Exit(fmt.Errorf("cannot open file for reading: %w", err), 1)
	}
	data, err = io.ReadAll(r)
	if err != nil {
		return cli.Exit(fmt.Errorf("cannot read data: %w", err), 1)
	}

	id := uuid.New().String()

	obj := conn.Object("com.redhat.Yggdrasil1", "/com/redhat/Yggdrasil1")
	if err := obj.Call("com.redhat.Yggdrasil1.Dispatch", dbus.Flags(0), c.String("worker"), id, metadata, data).Store(); err != nil {
		return cli.Exit(fmt.Errorf("cannot dispatch message: %w", err), 1)
	}

	fmt.Printf("Dispatched message %v to worker %v\n", id, c.String("worker"))

	return nil
}

func listenAction(ctx *cli.Context) error {
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

	if err := conn.AddMatchSignal(); err != nil {
		return cli.Exit(fmt.Errorf("cannot add match signal: %w", err), 1)
	}

	signals := make(chan *dbus.Signal)
	conn.Signal(signals)
	for s := range signals {
		switch s.Name {
		case "com.redhat.Yggdrasil1.WorkerEvent":
			worker, ok := s.Body[0].(string)
			if !ok {
				return cli.Exit(fmt.Errorf("cannot cast %T as string", s.Body[0]), 1)
			}
			name, ok := s.Body[1].(uint32)
			if !ok {
				return cli.Exit(fmt.Errorf("cannot cast %T as uint32", s.Body[1]), 1)
			}
			messageID, ok := s.Body[2].(string)
			if !ok {
				return cli.Exit(fmt.Errorf("cannot cast %T as string", s.Body[2]), 1)
			}
			responseTo, ok := s.Body[3].(string)
			if !ok {
				return cli.Exit(fmt.Errorf("cannot cast %T as string", s.Body[3]), 1)
			}
			var message string
			if len(s.Body) > 4 {
				message, ok = s.Body[4].(string)
				if !ok {
					return cli.Exit(fmt.Errorf("cannot cast %T as string", s.Body[4]), 1)
				}
			}
			log.Printf("%v: %v: %v: %v: %v", worker, messageID, responseTo, ipc.WorkerEventName(name), message)

		}
	}
	return nil
}

func generateMessage(
	messageType, responseTo, directive, content string,
	metadata map[string]string,
	version int,
) ([]byte, error) {
	switch messageType {
	case "data":
		msg, err := generateDataMessage(
			yggdrasil.MessageType(messageType),
			responseTo,
			directive,
			[]byte(content),
			metadata,
			version,
		)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		return data, nil
	case "command":
		msg, err := generateControlMessage(
			yggdrasil.MessageType(messageType),
			responseTo,
			version,
			[]byte(content),
		)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		return data, nil
	default:
		return nil, fmt.Errorf("unsupported message type: %v", messageType)
	}
}
