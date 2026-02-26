package gows

import (
	"context"
	"fmt"
	"github.com/devlikeapro/gows/mongostore"
	_ "github.com/lib/pq"           // PostgreSQL driver
	_ "github.com/mattn/go-sqlite3" // SQLite driver
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// storeContainer is the minimal interface shared by sqlstore.Container and
// mongostore.MongoContainer that GoWS needs.
type storeContainer interface {
	Close() error
}

// GoWS it's Go WebSocket or WhatSapp ;)
type GoWS struct {
	*whatsmeow.Client
	Context context.Context
	events  chan interface{}

	cancelContext context.CancelFunc
	container     storeContainer
}

var (
	clientVersionRegex = regexp.MustCompile(`"client_revision":(\d+),`)
	waVersionOnce      sync.Once
)

func (gows *GoWS) handleEvent(event interface{}) {
	var data interface{}
	switch event.(type) {
	case *events.Connected:
		// Populate the ConnectedEventData with the ID and PushName
		data = &ConnectedEventData{
			ID:       gows.Store.ID,
			PushName: gows.Store.PushName,
		}

	default:
		data = event
	}

	// reissue from events to client
	select {
	case <-gows.Context.Done():
		return
	case gows.events <- data:
	}
}

func (gows *GoWS) Start() error {
	gows.AddEventHandler(gows.handleEvent)

	// Not connected, listen for QR code events
	if gows.Store.ID == nil {
		gows.listenQRCodeEvents()
	}

	return gows.Connect()
}

func (gows *GoWS) listenQRCodeEvents() {
	// No ID stored, new login
	qrChan, _ := gows.GetQRChannel(gows.Context)

	// reissue from QrChan to events
	go func() {
		for {
			select {
			case <-gows.Context.Done():
				return
			case qr := <-qrChan:
				// If the event is empty, we should stop the goroutine
				if qr.Event == "" {
					return
				}
				gows.events <- qr
			}
		}
	}()
}

func (gows *GoWS) Stop() {
	gows.Disconnect()
	gows.cancelContext()
	err := gows.container.Close()
	if err != nil {
		gows.Log.Errorf("Error closing container: %v", err)
	}
	close(gows.events)
}

func (gows *GoWS) GetOwnId() types.JID {
	if gows == nil {
		return types.EmptyJID
	}
	id := gows.Store.ID
	if id == nil {
		return types.EmptyJID
	}
	return *id
}

type ConnectedEventData struct {
	ID       *types.JID
	PushName string
}

func BuildSession(ctx context.Context, log waLog.Logger, dialect string, address string) (*GoWS, error) {
	waVersionOnce.Do(func() {
		if err := refreshLatestWAVersion(); err != nil {
			log.Warnf("Failed to refresh WA web version, falling back to built-in value: %v", err)
			return
		}
		log.Infof("WA web version refreshed to %v", store.GetWAVersion())
	})

	// Prepare the database — either MongoDB or a SQL backend (SQLite / PostgreSQL).
	var container storeContainer
	var deviceStore *store.Device

	if dialect == "mongodb" {
		mc, err := mongostore.New(address, log.Sub("Database"))
		if err != nil {
			return nil, err
		}
		ds, err := mc.GetFirstDevice()
		if err != nil {
			_ = mc.Close()
			return nil, err
		}
		container = mc
		deviceStore = ds
	} else {
		sc, err := sqlstore.New(context.Background(), dialect, address, log.Sub("Database"))
		if err != nil {
			return nil, err
		}
		ds, err := sc.GetFirstDevice(context.Background())
		if err != nil {
			_ = sc.Close()
			return nil, err
		}
		container = sc
		deviceStore = ds
	}

	// Configure the WhatsApp client
	client := whatsmeow.NewClient(deviceStore, log.Sub("Client"))
	client.AutomaticMessageRerequestFromPhone = true
	client.EmitAppStateEventsOnFullSync = true

	ctx, cancel := context.WithCancel(ctx)
	gows := GoWS{
		client,
		ctx,
		make(chan interface{}, 10),
		cancel,
		container,
	}
	return &gows, nil
}

func refreshLatestWAVersion() error {
	req, err := http.NewRequest(http.MethodGet, "https://web.whatsapp.com", nil)
	if err != nil {
		return fmt.Errorf("prepare request: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36")
	req.Header.Set("Sec-Fetch-Dest", "document")
	req.Header.Set("Sec-Fetch-Mode", "navigate")
	req.Header.Set("Sec-Fetch-Site", "none")
	req.Header.Set("Sec-Fetch-User", "?1")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	match := clientVersionRegex.FindSubmatch(data)
	if len(match) == 0 {
		return fmt.Errorf("client revision not found in response")
	}
	revision, err := strconv.ParseUint(string(match[1]), 10, 32)
	if err != nil {
		return fmt.Errorf("parse revision: %w", err)
	}

	store.SetWAVersion(store.WAVersionContainer{2, 3000, uint32(revision)})
	return nil
}

func (gows *GoWS) GetEventChannel() <-chan interface{} {
	return gows.events
}

func (gows *GoWS) SendMessage(ctx context.Context, to types.JID, msg *waE2E.Message, extra ...whatsmeow.SendRequestExtra) (resp whatsmeow.SendResponse, err error) {
	resp, err = gows.Client.SendMessage(ctx, to, msg, extra...)
	if err != nil {
		return
	}
	info := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     to,
			Sender:   gows.GetOwnId(),
			IsFromMe: true,
			IsGroup:  to.Server == types.GroupServer,
		},
		ID:        resp.ID,
		Timestamp: resp.Timestamp,
		ServerID:  resp.ServerID,
	}
	evt := &events.Message{Info: *info, Message: msg, RawMessage: msg}
	go gows.handleEvent(evt)
	return
}
