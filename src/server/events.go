package server

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/devlikeapro/gows/gows"
	"github.com/devlikeapro/gows/proto"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/grpc"
	"reflect"
	"strings"
	"time"
)

func (s *Server) safeMarshal(v interface{}) (result string) {
	defer func() {
		if err := recover(); err != nil {
			// Print log error and ignore
			s.log.Errorf("Panic happened when marshaling data: %v", err)
			result = ""
		}
	}()
	data, err := json.Marshal(v)
	if err != nil {
		s.log.Errorf("Error when marshaling data: %v", err)
		return ""
	}
	result = string(data)
	return result
}

func (s *Server) StreamEvents(req *__.StreamEventsRequest, stream grpc.ServerStreamingServer[__.EventJson]) error {
	session := req.GetSession()
	if session == nil || session.GetId() == "" {
		return fmt.Errorf("session id is required")
	}
	name := session.GetId()
	excluded := make(map[string]struct{}, len(req.GetExclude()))
	for _, eventName := range req.GetExclude() {
		excluded[eventName] = struct{}{}
	}
	streamId := uuid.New()
	listener := s.addListener(name, streamId)
	defer s.removeListener(name, streamId)
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case event := <-listener:
			// Remove * at the start if it's *
			eventType := reflect.TypeOf(event).String()
			eventType = strings.TrimPrefix(eventType, "*")
			if _, skip := excluded[eventType]; skip {
				continue
			}

			jsonString := s.safeMarshal(event)
			if jsonString == "" {
				continue
			}

			data := __.EventJson{
				Session: name,
				Event:   eventType,
				Data:    jsonString,
			}
			err := stream.Send(&data)
			if err != nil {
				return err
			}
		}
	}
}

func (s *Server) IssueEvent(session string, event interface{}) {
	s.storeMessageEvent(session, event)

	listeners := s.getListeners(session)
	for _, listener := range listeners {
		go func() {
			defer func() {
				if err := recover(); err != nil {
					// Print log error and ignore
					fmt.Print("Error when sending event to listener: ", err)
				}
			}()
			listener <- event
		}()
	}
}

type messageEventWriter interface {
	PutMessageEvent(
		ctx context.Context,
		chatJID string,
		messageID string,
		timestampMS int64,
		fromMe bool,
		status uint32,
		data string,
	) error
}

func (s *Server) storeMessageEvent(session string, event interface{}) {
	cli, writer := s.getMessageEventWriter(session)
	if cli == nil || writer == nil {
		return
	}

	switch evt := event.(type) {
	case *events.Message:
		s.storeLiveMessageEvent(writer, evt)
	case *events.HistorySync:
		s.storeHistorySyncMessages(writer, evt)
	}
}

func (s *Server) getMessageEventWriter(session string) (*gows.GoWS, messageEventWriter) {
	cli, err := s.Sm.Get(session)
	if err != nil {
		return nil, nil
	}
	writer, ok := cli.Store.LIDs.(messageEventWriter)
	if !ok {
		return nil, nil
	}
	return cli, writer
}

func (s *Server) storeLiveMessageEvent(writer messageEventWriter, msg *events.Message) {
	if msg == nil || msg.Info.Chat.User == "" || msg.Info.ID == "" {
		return
	}

	payload := s.safeMarshal(msg)
	if payload == "" {
		return
	}

	timestampMS := msg.Info.Timestamp.UnixMilli()
	if timestampMS <= 0 {
		timestampMS = time.Now().UnixMilli()
	}

	_ = writer.PutMessageEvent(
		context.Background(),
		msg.Info.Chat.ToNonAD().String(),
		string(msg.Info.ID),
		timestampMS,
		msg.Info.IsFromMe,
		0,
		payload,
	)
}

func (s *Server) storeHistorySyncMessages(writer messageEventWriter, history *events.HistorySync) {
	if history == nil || history.Data == nil {
		return
	}

	for _, conversation := range history.Data.GetConversations() {
		for _, historyMsg := range conversation.GetMessages() {
			webMsg := historyMsg.GetMessage()
			if webMsg == nil || webMsg.GetKey() == nil || webMsg.GetMessage() == nil {
				continue
			}

			key := webMsg.GetKey()
			messageID := strings.TrimSpace(key.GetID())
			if messageID == "" {
				continue
			}

			chatRaw := strings.TrimSpace(key.GetRemoteJID())
			if chatRaw == "" {
				chatRaw = strings.TrimSpace(conversation.GetID())
			}
			chatJID, err := types.ParseJID(chatRaw)
			if err != nil {
				continue
			}
			chatJID = chatJID.ToNonAD()

			senderRaw := strings.TrimSpace(key.GetParticipant())
			if senderRaw == "" {
				senderRaw = chatRaw
			}
			senderJID, senderErr := types.ParseJID(senderRaw)
			if senderErr == nil {
				senderRaw = senderJID.ToNonAD().String()
			}

			timestampSec := int64(webMsg.GetMessageTimestamp())
			if timestampSec <= 0 {
				timestampSec = int64(conversation.GetConversationTimestamp())
			}
			if timestampSec <= 0 {
				timestampSec = int64(conversation.GetLastMsgTimestamp())
			}
			if timestampSec <= 0 {
				timestampSec = time.Now().Unix()
			}
			timestampMS := timestampSec * 1000

			payload := s.safeMarshal(map[string]interface{}{
				"Info": map[string]interface{}{
					"Chat":      chatJID.String(),
					"Sender":    senderRaw,
					"ID":        messageID,
					"Timestamp": time.Unix(timestampSec, 0).UTC().Format(time.RFC3339),
					"IsFromMe":  key.GetFromMe(),
					"IsGroup":   chatJID.Server == types.GroupServer,
				},
				"Message": webMsg.GetMessage(),
				"Status":  int32(webMsg.GetStatus()),
			})
			if payload == "" {
				continue
			}

			_ = writer.PutMessageEvent(
				context.Background(),
				chatJID.String(),
				messageID,
				timestampMS,
				key.GetFromMe(),
				uint32(webMsg.GetStatus()),
				payload,
			)
		}
	}
}

func (s *Server) addListener(session string, id uuid.UUID) chan interface{} {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()

	listener := make(chan interface{}, 10)
	sessionListeners, ok := s.listeners[session]
	if !ok {
		sessionListeners = map[uuid.UUID]chan interface{}{}
		s.listeners[session] = sessionListeners
	}
	sessionListeners[id] = listener
	return listener
}

func (s *Server) removeListener(session string, id uuid.UUID) {
	s.listenersLock.Lock()
	defer s.listenersLock.Unlock()
	listener, ok := s.listeners[session][id]
	if !ok {
		return
	}
	delete(s.listeners[session], id)
	// if it's the last listener, remove the session
	if len(s.listeners[session]) == 0 {
		delete(s.listeners, session)
	}
	close(listener)
}

func (s *Server) getListeners(session string) []chan interface{} {
	s.listenersLock.RLock()
	defer s.listenersLock.RUnlock()
	listeners := make([]chan interface{}, 0, len(s.listeners))
	for _, listener := range s.listeners[session] {
		listeners = append(listeners, listener)
	}
	return listeners
}
