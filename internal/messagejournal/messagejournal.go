package messagejournal

import (
	"bytes"
	"database/sql"
	"fmt"
	"text/template"
	"time"

	"git.sr.ht/~spc/go-log"
	_ "github.com/mattn/go-sqlite3"
	"github.com/redhatinsights/yggdrasil"
	"github.com/redhatinsights/yggdrasil/ipc"
)

const runtimeTableName string = "runtime"
const persistentTableName string = "persistent"

// MessageJournal is a data structure representing the collection
// of message journal entries received from worker emitted events and messages.
type MessageJournal struct {
	database *sql.DB
}

// Filter is a data structure representing the filtering options
// that are used when message journal entries are retrieved by yggctl.
type Filter struct {
	Persistent     bool
	TruncateLength int
	MessageID      string
	Worker         string
	From           string
	To             string
}

// New initializes a message journal sqlite database consisting
// of a runtime table that gets cleared on every session start
// and a persistent table that maintains journal entries across sessions.
func New(databaseFilePath string) (*MessageJournal, error) {
	const dropTableTemplate string = "DROP TABLE IF EXISTS %s"
	const createTableTemplate string = `CREATE TABLE IF NOT EXISTS %s (
		id INTEGER NOT NULL PRIMARY KEY,
		message_id VARCHAR(36) NOT NULL,
		sent DATETIME NOT NULL,
		worker_name VARCHAR(128) NOT NULL,
		response_to VARCHAR(36),
		worker_event INTEGER,
		worker_message TEXT
	);`

	var err error
	db, err := sql.Open("sqlite3", databaseFilePath)
	if err != nil {
		return nil, fmt.Errorf("database object not created: %w", err)
	}
	messageJournal := MessageJournal{database: db}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("message journal database not connected: %w", err)
	}

	if _, err = db.Exec(fmt.Sprintf(dropTableTemplate, runtimeTableName)); err != nil {
		return nil, fmt.Errorf("runtime messsage journal table not deleted: %w", err)
	}
	if _, err = db.Exec(fmt.Sprintf(createTableTemplate, runtimeTableName)); err != nil {
		return nil, fmt.Errorf("runtime messsage journal table not created: %w", err)
	}
	if _, err = db.Exec(fmt.Sprintf(createTableTemplate, persistentTableName)); err != nil {
		return nil, fmt.Errorf("persistent messsage journal table not created: %w", err)
	}

	return &messageJournal, nil
}

// AddEntry adds a new message journal entry to both the temporary runtime table
// which gets cleared at the start of every session and the persistent table
// which maintains message entries across multiple sessions.
func (j *MessageJournal) AddEntry(entry yggdrasil.WorkerMessage) (*yggdrasil.WorkerMessage, error) {
	const insertEntryTemplate string = `INSERT INTO %s (
		message_id, sent, worker_name, response_to, worker_event, worker_message) 
		values (?,?,?,?,?,?)`

	runtimeAction, err := j.database.Prepare(fmt.Sprintf(insertEntryTemplate, runtimeTableName))
	if err != nil {
		return nil, fmt.Errorf("cannot prepare statement for table '%v': %w", runtimeTableName, err)
	}
	persistentAction, err := j.database.Prepare(fmt.Sprintf(insertEntryTemplate, persistentTableName))
	if err != nil {
		return nil, fmt.Errorf("cannot prepare statement for table '%v': %w", persistentTableName, err)
	}

	runtimeResult, err := runtimeAction.Exec(
		entry.MessageID,
		entry.Sent,
		entry.WorkerName,
		entry.ResponseTo,
		entry.WorkerEvent.EventName,
		entry.WorkerEvent.EventMessage,
	)
	if err != nil {
		return nil, fmt.Errorf("could not insert journal entry into table '%v': %w", runtimeTableName, err)
	}
	persistentResult, err := persistentAction.Exec(
		entry.MessageID,
		entry.Sent,
		entry.WorkerName,
		entry.ResponseTo,
		entry.WorkerEvent.EventName,
		entry.WorkerEvent.EventMessage,
	)
	if err != nil {
		return nil, fmt.Errorf("could not insert journal entry into table '%v': %w", persistentTableName, err)
	}

	runtimeEntryID, err := runtimeResult.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("could not select last insert ID '%v' for table '%v': %w", runtimeEntryID, runtimeTableName, err)
	}
	log.Debugf("new message journal entry (id: %v) added to table: '%v'", runtimeEntryID, runtimeTableName)

	persistentEntryID, err := persistentResult.LastInsertId()
	if err != nil {
		return nil, fmt.Errorf("could not select last insert ID '%v' for table '%v': %w", persistentEntryID, persistentTableName, err)
	}
	log.Debugf("new message journal entry (id: %v) added to table: '%v'", persistentEntryID, persistentTableName)

	return &entry, nil
}

// GetEntries retrieves a list of all the journal entries in the message journal database
// that meet the criteria of the provided message journal filter.
func (j *MessageJournal) GetEntries(filter Filter) ([]map[string]string, error) {
	entries := []map[string]string{}
	queryString, err := j.buildDynamicGetEntriesQuery(filter)
	if err != nil {
		return nil, fmt.Errorf("cannot build dynamic sql query: %w", err)
	}

	preparedQuery, err := j.database.Prepare(queryString)
	if err != nil {
		return nil, fmt.Errorf("cannot prepare query when retrieving journal entries: %w", err)
	}

	rows, err := preparedQuery.Query()
	if err != nil {
		return nil, fmt.Errorf("cannot execute query to retrieve journal entries: %w", err)
	}

	for rows.Next() {
		var rowID int
		var messageID string
		var sent time.Time
		var workerName string
		var responseTo string
		var workerEvent uint
		var workerEventMessage string

		err := rows.Scan(
			&rowID,
			&messageID,
			&sent,
			&workerName,
			&responseTo,
			&workerEvent,
			&workerEventMessage,
		)
		if err != nil {
			return nil, fmt.Errorf("cannot scan journal entry columns: %w", err)
		}

		// Truncate the worker messages by the truncate length specified.
		messageMaxSize := len(workerEventMessage)
		if messageMaxSize >= filter.TruncateLength {
			messageMaxSize = filter.TruncateLength
			workerEventMessage = fmt.Sprintf("%+v...", workerEventMessage[:messageMaxSize])
		}

		// Convert the entry properties into a string format and append to the list of entries.
		newMessage := map[string]string{
			"message_id":     messageID,
			"sent":           sent.String(),
			"worker_name":    workerName,
			"response_to":    responseTo,
			"worker_event":   ipc.WorkerEventName(workerEvent).String(),
			"worker_message": workerEventMessage,
		}
		entries = append(entries, newMessage)
	}
	rowIterationErr := rows.Err()
	if rowIterationErr != nil {
		return nil, fmt.Errorf("cannot iterate queried journal entries: %w", rowIterationErr)
	}
	closeErr := rows.Close()
	if closeErr != nil {
		return nil, fmt.Errorf("cannot close journal entry rows: %w", closeErr)
	}

	return entries, nil
}

// buildDynamicGetEntriesQuery is a utility method that builds the dynamic sql query
// required to filter journal entry messages from the message journal database
// when they are retrieved in the 'GetEntries' method.
func (j *MessageJournal) buildDynamicGetEntriesQuery(filter Filter) (string, error) {
	var tableName string
	if filter.Persistent {
		tableName = "persistent"
	} else {
		tableName = "runtime"
	}

	queryTemplate := template.New("dynamicGetEntriesQuery")
	queryTemplateParse, err := queryTemplate.Parse(
		`SELECT * FROM {{.Table}}
		{{if .MessageID}} INTERSECT SELECT * FROM {{.Table}} WHERE message_id='{{.MessageID}}'{{end}}
		{{if .Worker}} INTERSECT SELECT * FROM {{.Table}} WHERE worker_name='{{.Worker}}'{{end}}
		{{if .From}} INTERSECT SELECT * FROM {{.Table}} WHERE sent>='{{.From}}'{{end}}
		{{if .To}} INTERSECT SELECT * FROM {{.Table}} WHERE sent<='{{.To}}'{{end}}
		ORDER BY sent`,
	)
	if err != nil {
		return "", fmt.Errorf("cannot parse query template parameters: %w", err)
	}
	var compiledQuery bytes.Buffer
	queryCompileErr := queryTemplateParse.Execute(&compiledQuery,
		struct {
			Table     string
			MessageID string
			Worker    string
			From      string
			To        string
		}{
			tableName, filter.MessageID, filter.Worker, filter.From, filter.To,
		})
	if queryCompileErr != nil {
		return "", fmt.Errorf("cannot compile query template: %w", queryCompileErr)
	}
	compiledQueryAsString := compiledQuery.String()
	return compiledQueryAsString, nil
}
