package messagejournal

import (
	"fmt"
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
	tests := []struct {
		description string
		input       string
	}{
		{
			description: "create message journal",
			input:       "file::memory:?cache=shared",
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
			wantError: fmt.Errorf("no journal entries found"),
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
			wantError: fmt.Errorf("no journal entries found"),
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
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			// Create a message journal to test with:
			journal, err := New("file::memory:?cache=shared")
			if err != nil {
				t.Fatal(err)
			}

			// Add entries from test input data:
			for _, entry := range test.entries {
				if err := journal.AddEntry(entry); err != nil {
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
	}
}

func TestAddEntry(t *testing.T) {
	tests := []struct {
		description string
		input       yggdrasil.WorkerMessage
		wantError   error
	}{
		{
			description: "create journal entry",
			input:       placeholderWorkerMessageEntry,
		},
	}

	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			messageJournal, err := New("file::memory:?cache=shared")
			if err != nil {
				t.Fatal(err)
			}

			err = messageJournal.AddEntry(test.input)
			if test.wantError != nil {
				if !cmp.Equal(err, test.wantError, cmpopts.EquateErrors()) {
					t.Errorf("%#v != %#v", err, test.wantError)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
