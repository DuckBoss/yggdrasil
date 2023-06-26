package messagejournal

import (
	"bytes"
	"database/sql"
	"embed"
	"fmt"
	"text/template"
	"time"

	"git.sr.ht/~spc/go-log"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	_ "github.com/mattn/go-sqlite3"
	"github.com/redhatinsights/yggdrasil"
	"github.com/redhatinsights/yggdrasil/ipc"
)

const messageJournalTableName string = "journal"

//go:embed migrations/*.sql
var embeddedMigrationData embed.FS

// MessageJournal is a data structure representing the collection
// of message journal entries received from worker emitted events and messages.
// It also stores the date time of when the journal was initialized to track
// events and messages in the active session.
type MessageJournal struct {
	database      *sql.DB
	initializedAt time.Time
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
	db, err := sql.Open("sqlite3", databaseFilePath)
	if err != nil {
		return nil, fmt.Errorf("database object not created: %w", err)
	}
	if err = migrateMessageJournalDB(db, databaseFilePath); err != nil {
		return nil, fmt.Errorf("database migration error: %w", err)
	}

	messageJournal := MessageJournal{database: db, initializedAt: time.Now().UTC()}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("message journal database not connected: %w", err)
	}

	return &messageJournal, nil
}

// migrateMessageJournalDB handles the migration of the message journal
// database and ensures the schema is up to date on each session start.
func migrateMessageJournalDB(db *sql.DB, databaseFilePath string) error {
	databaseDriver, err := sqlite.WithInstance(db, &sqlite.Config{})
	if err != nil {
		return fmt.Errorf("database driver not initialized: %w", err)
	}
	migrationDriver, err := iofs.New(embeddedMigrationData, "migrations")
	if err != nil {
		return fmt.Errorf("embedded migration data not found: %w", err)
	}
	migration, err := migrate.NewWithInstance(
		"iofs",
		migrationDriver,
		databaseFilePath,
		databaseDriver,
	)
	if err != nil {
		return fmt.Errorf("database migration not initialized: %w", err)
	}
	if err = migration.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("database migration failed: %w", err)
	}
	return nil
}

// AddEntry adds a new message journal entry to both the temporary runtime table
// which gets cleared at the start of every session and the persistent table
// which maintains message entries across multiple sessions.
func (j *MessageJournal) AddEntry(entry yggdrasil.WorkerMessage) error {
	const insertEntryTemplate string = `INSERT INTO %s (
		message_id, sent, worker_name, response_to, worker_event, worker_message) 
		values (?,?,?,?,?,?)`

	insertAction, err := j.database.Prepare(fmt.Sprintf(insertEntryTemplate, messageJournalTableName))
	if err != nil {
		return fmt.Errorf("cannot prepare statement for table '%v': %w", messageJournalTableName, err)
	}

	persistentResult, err := insertAction.Exec(
		entry.MessageID,
		entry.Sent,
		entry.WorkerName,
		entry.ResponseTo,
		entry.WorkerEvent.EventName,
		entry.WorkerEvent.EventMessage,
	)
	if err != nil {
		return fmt.Errorf("could not insert journal entry into table '%v': %w", messageJournalTableName, err)
	}

	entryID, err := persistentResult.LastInsertId()
	if err != nil {
		return fmt.Errorf("could not select last insert ID '%v' for table '%v': %w", entryID, messageJournalTableName, err)
	}
	log.Debugf("new message journal entry (id: %v) added: '%v'", entryID, entry.MessageID)

	return nil
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
		if messageMaxSize >= filter.TruncateLength && filter.TruncateLength > 0 {
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
	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("cannot iterate queried journal entries: %w", err)
	}
	err = rows.Close()
	if err != nil {
		return nil, fmt.Errorf("cannot close journal entry rows: %w", err)
	}

	return entries, nil
}

// buildDynamicGetEntriesQuery is a utility method that builds the dynamic sql query
// required to filter journal entry messages from the message journal database
// when they are retrieved in the 'GetEntries' method.
func (j *MessageJournal) buildDynamicGetEntriesQuery(filter Filter) (string, error) {
	queryTemplate := template.New("dynamicGetEntriesQuery")
	queryTemplateParse, err := queryTemplate.Parse(
		`SELECT * FROM {{.Table}}
		{{if .MessageID}} INTERSECT SELECT * FROM {{.Table}} WHERE message_id='{{.MessageID}}'{{end}}
		{{if .Worker}} INTERSECT SELECT * FROM {{.Table}} WHERE worker_name='{{.Worker}}'{{end}}
		{{if .From}} INTERSECT SELECT * FROM {{.Table}} WHERE sent>='{{.From}}'{{end}}
		{{if .To}} INTERSECT SELECT * FROM {{.Table}} WHERE sent<='{{.To}}'{{end}}
		{{if not .Persistent}} INTERSECT SELECT * FROM {{.Table}} WHERE sent>='{{.InitializedAt}}'{{end}}
		ORDER BY sent`,
	)
	if err != nil {
		return "", fmt.Errorf("cannot parse query template parameters: %w", err)
	}
	var compiledQuery bytes.Buffer
	err = queryTemplateParse.Execute(&compiledQuery,
		struct {
			Table         string
			InitializedAt string
			Persistent    bool
			MessageID     string
			Worker        string
			From          string
			To            string
		}{
			messageJournalTableName, j.initializedAt.String(), filter.Persistent,
			filter.MessageID, filter.Worker, filter.From, filter.To,
		})
	if err != nil {
		return "", fmt.Errorf("cannot compile query template: %w", err)
	}
	compiledQueryAsString := compiledQuery.String()
	return compiledQueryAsString, nil
}
