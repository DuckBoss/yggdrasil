package messagejournal

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/redhatinsights/yggdrasil"
)

var placeholderWorkerMessageEntry = yggdrasil.WorkerMessage{
	MessageID:  "test-id",
	Sent:       time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
	WorkerName: "test-worker",
	ResponseTo: "test-response",
	WorkerEvent: struct {
		EventName    uint   "json:\"event_name\""
		EventMessage string "json:\"event_message\""
	}{
		EventName:    0,
		EventMessage: "test-event-message",
	},
}

func TestNew(t *testing.T) {
	tempDir, err := ioutil.TempDir("/tmp/", "yggd-message-journal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	tests := []struct {
		description string
		input       string
	}{
		{
			description: "create message journal",
			input:       filepath.Join(tempDir, "messagejournal-test.db"),
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			got, err := New(test.input)

			if err != nil {
				t.Fatal(err)
			}
			if got == nil {
				t.Errorf("message journal is null")
			}
		})
		t.Cleanup(func() {
			os.RemoveAll(filepath.Join(tempDir, "messagejournal-test.db"))
		})
	}
}

func TestGetEntries(t *testing.T) {
	tests := []struct {
		description string
		entries     []yggdrasil.WorkerMessage
		input       Filter
		want        []map[string]string
		wantError   error
	}{
		{
			description: "get journal entries - unfiltered empty",
			entries:     []yggdrasil.WorkerMessage{},
			input: Filter{
				Persistent:     false,
				TruncateLength: 100,
				MessageID:      "",
				Worker:         "",
				From:           "",
				To:             "",
			},
			want: []map[string]string{},
		},
		{
			description: "get journal entries - unfiltered results",
			entries: []yggdrasil.WorkerMessage{
				placeholderWorkerMessageEntry,
			},
			input: Filter{
				Persistent:     false,
				TruncateLength: 100,
				MessageID:      "",
				Worker:         "",
				From:           "",
				To:             "",
			},
			want: []map[string]string{
				0: {
					"message_id":     "test-id",
					"response_to":    "test-response",
					"sent":           "2000-01-01 00:00:00 +0000 UTC",
					"worker_event":   "",
					"worker_message": "test-event-message",
					"worker_name":    "test-worker",
				},
			},
		},
		{
			description: "get journal entries - filtered empty",
			entries: []yggdrasil.WorkerMessage{
				placeholderWorkerMessageEntry,
			},
			input: Filter{
				Persistent:     false,
				TruncateLength: 100,
				MessageID:      "test-invalid-filtered-message-id",
				Worker:         "",
				From:           "",
				To:             "",
			},
			want: []map[string]string{},
		},
		{
			description: "get journal entries - filtered results",
			entries: []yggdrasil.WorkerMessage{
				placeholderWorkerMessageEntry,
				{
					MessageID:  "test-filtered-message-id",
					Sent:       time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC),
					WorkerName: "test-worker",
					ResponseTo: "test-response",
					WorkerEvent: struct {
						EventName    uint   "json:\"event_name\""
						EventMessage string "json:\"event_message\""
					}{
						EventName:    0,
						EventMessage: "test-event-message",
					},
				},
			},
			input: Filter{
				Persistent:     false,
				TruncateLength: 100,
				MessageID:      "test-filtered-message-id",
				Worker:         "",
				From:           "",
				To:             "",
			},
			want: []map[string]string{
				0: {
					"message_id":     "test-filtered-message-id",
					"response_to":    "test-response",
					"sent":           "2000-01-01 00:00:00 +0000 UTC",
					"worker_event":   "",
					"worker_message": "test-event-message",
					"worker_name":    "test-worker",
				},
			},
		},
	}

	tempDir, err := ioutil.TempDir("/tmp/", "yggd-message-journal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			// Create a message journal to test with:
			journal, err := New(filepath.Join(tempDir, "messagejournal-test.db"))
			if err != nil {
				t.Fatal(err)
			}

			// Add entries from test input data:
			for _, entry := range test.entries {
				_, err := journal.AddEntry(entry)
				if err != nil {
					t.Fatal(err)
				}
			}

			// Get entries from the message journal:
			got, err := journal.GetEntries(test.input)
			if test.wantError != nil {
				if !cmp.Equal(err, test.wantError, cmpopts.EquateErrors()) {
					t.Errorf("%#v != %#v", err, test.wantError)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !cmp.Equal(got, test.want) {
					t.Errorf("%#v != %#v", got, test.want)
				}
			}
		})
		t.Cleanup(func() {
			os.RemoveAll(tempDir)
		})
	}
}

func TestAddEntry(t *testing.T) {
	tests := []struct {
		description string
		input       yggdrasil.WorkerMessage
		want        yggdrasil.WorkerMessage
		wantError   error
	}{
		{
			description: "create journal entry",
			input:       placeholderWorkerMessageEntry,
			want:        placeholderWorkerMessageEntry,
		},
	}

	tempDir, err := ioutil.TempDir("/tmp/", "yggd-message-journal-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			messageJournal, err := New(filepath.Join(tempDir, "messagejournal-test.db"))
			if err != nil {
				t.Fatal(err)
			}

			got, err := messageJournal.AddEntry(test.input)

			if test.wantError != nil {
				if !cmp.Equal(err, test.wantError, cmpopts.EquateErrors()) {
					t.Errorf("%#v != %#v", err, test.wantError)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !cmp.Equal(*got, test.want) {
					t.Errorf("%#v != %#v", *got, test.want)
				}
			}
		})
		t.Cleanup(func() {
			os.RemoveAll(filepath.Join(tempDir, "messagejournal-test.db"))
		})
	}
}
