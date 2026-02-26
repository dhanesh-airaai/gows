package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/devlikeapro/gows/media"
	"github.com/devlikeapro/gows/proto"
	"github.com/golang/protobuf/proto"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"os"
	"strconv"
	"strings"
	"time"
)

func (s *Server) SendMessage(ctx context.Context, req *__.MessageRequest) (*__.MessageResponse, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.GetJid())
	if err != nil {
		return nil, err
	}

	message := waE2E.Message{}
	mediaResponse := whatsmeow.UploadResponse{}
	thumbnailTimeout := timeoutFromEnv("GOWS_THUMBNAIL_TIMEOUT", 8*time.Second)
	uploadTimeout := timeoutFromEnv("GOWS_MEDIA_UPLOAD_TIMEOUT", 120*time.Second)
	sendTimeout := timeoutFromEnv("GOWS_SEND_MESSAGE_TIMEOUT", 45*time.Second)

	if req.Media == nil {
		var backgroundArgb *uint32
		if req.BackgroundColor != nil {
			backgroundArgb, err = media.ParseColor(req.BackgroundColor.Value)
			if err != nil {
				return nil, err
			}
		}

		var font *waE2E.ExtendedTextMessage_FontType
		if req.Font != nil {
			font = media.ParseFont(req.Font.Value)
		}

		message.ExtendedTextMessage = &waE2E.ExtendedTextMessage{
			Text:           proto.String(req.Text),
			BackgroundArgb: backgroundArgb,
			Font:           font,
		}
	} else {
		if len(req.Media.Content) == 0 {
			return nil, errors.New("media content is empty")
		}

		uploadWithTimeout := func(mediaType whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
			uploadCtx, cancel := withOperationTimeout(ctx, uploadTimeout)
			defer cancel()
			resp, uploadErr := cli.UploadMedia(uploadCtx, jid, req.Media.Content, mediaType)
			if uploadErr != nil {
				return whatsmeow.UploadResponse{}, fmt.Errorf("upload media (%v): %w", mediaType, uploadErr)
			}
			return resp, nil
		}

		var mediaType whatsmeow.MediaType
		switch req.Media.Type {
		case __.MediaType_IMAGE:
			// Upload
			mediaType = whatsmeow.MediaImage
			mediaResponse, err = uploadWithTimeout(mediaType)
			if err != nil {
				return nil, err
			}

			// Generate Thumbnail
			thumbnail, err := runBytesWithTimeout(thumbnailTimeout, func() ([]byte, error) {
				return media.ImageThumbnail(req.Media.Content)
			})
			if err != nil {
				s.log.Errorf("Failed to generate thumbnail: %v", err)
			}
			// Attach
			message.ImageMessage = &waE2E.ImageMessage{
				Caption:       proto.String(req.Text),
				Mimetype:      proto.String(req.Media.Mimetype),
				JPEGThumbnail: thumbnail,
				URL:           &mediaResponse.URL,
				DirectPath:    &mediaResponse.DirectPath,
				FileSHA256:    mediaResponse.FileSHA256,
				FileLength:    &mediaResponse.FileLength,
				MediaKey:      mediaResponse.MediaKey,
				FileEncSHA256: mediaResponse.FileEncSHA256,
			}
		case __.MediaType_AUDIO:
			mediaType = whatsmeow.MediaAudio
			var waveform []byte
			var duration float32
			// Get waveform and duration if available
			if req.Media.Audio != nil {
				waveform = req.Media.Audio.Waveform
				duration = req.Media.Audio.Duration
			}

			if waveform == nil || len(waveform) == 0 {
				// Generate waveform
				waveform, err = media.Waveform(req.Media.Content)
				if err != nil {
					s.log.Errorf("Failed to generate waveform: %v", err)
				}
			}
			if duration == 0 {
				// Get duration
				duration, err = media.Duration(req.Media.Content)
				if err != nil {
					s.log.Errorf("Failed to get duration of audio: %v", err)
				}
			}
			durationSeconds := uint32(duration)

			// Upload
			mediaResponse, err = uploadWithTimeout(mediaType)
			if err != nil {
				return nil, err
			}

			// Attach
			ptt := true
			message.AudioMessage = &waE2E.AudioMessage{
				Mimetype:      proto.String(req.Media.Mimetype),
				URL:           &mediaResponse.URL,
				DirectPath:    &mediaResponse.DirectPath,
				MediaKey:      mediaResponse.MediaKey,
				FileEncSHA256: mediaResponse.FileEncSHA256,
				FileSHA256:    mediaResponse.FileSHA256,
				FileLength:    &mediaResponse.FileLength,
				Seconds:       &durationSeconds,
				Waveform:      waveform,
				PTT:           &ptt,
			}
		case __.MediaType_VIDEO:
			mediaType = whatsmeow.MediaVideo
			// Upload
			mediaResponse, err = uploadWithTimeout(mediaType)
			if err != nil {
				return nil, err
			}

			// Generate Thumbnail
			thumbnail, err := runBytesWithTimeout(thumbnailTimeout, func() ([]byte, error) {
				return media.VideoThumbnail(
					req.Media.Content,
					0,
					struct{ Width int }{Width: 72},
				)
			})

			if err != nil {
				s.log.Infof("Failed to generate video thumbnail: %v", err)
			}

			message.VideoMessage = &waE2E.VideoMessage{
				Caption:       proto.String(req.Text),
				Mimetype:      proto.String(req.Media.Mimetype),
				URL:           &mediaResponse.URL,
				DirectPath:    &mediaResponse.DirectPath,
				MediaKey:      mediaResponse.MediaKey,
				FileEncSHA256: mediaResponse.FileEncSHA256,
				FileSHA256:    mediaResponse.FileSHA256,
				FileLength:    &mediaResponse.FileLength,
				JPEGThumbnail: thumbnail,
			}

		case __.MediaType_DOCUMENT:
			mediaType = whatsmeow.MediaDocument
			// Upload
			mediaResponse, err = uploadWithTimeout(mediaType)
			if err != nil {
				return nil, err
			}

			// Generate thumbnail only for image-like documents.
			thumbnail := []byte(nil)
			mimetype := strings.ToLower(req.Media.Mimetype)
			if strings.HasPrefix(mimetype, "image/") {
				thumbnail, err = runBytesWithTimeout(thumbnailTimeout, func() ([]byte, error) {
					return media.ImageThumbnail(req.Media.Content)
				})
				if err != nil {
					s.log.Infof("Failed to generate document thumbnail: %v", err)
				}
			}

			// Attach
			message.DocumentMessage = &waE2E.DocumentMessage{
				Caption:       proto.String(req.Text),
				Mimetype:      proto.String(req.Media.Mimetype),
				URL:           &mediaResponse.URL,
				DirectPath:    &mediaResponse.DirectPath,
				MediaKey:      mediaResponse.MediaKey,
				FileEncSHA256: mediaResponse.FileEncSHA256,
				FileSHA256:    mediaResponse.FileSHA256,
				FileLength:    &mediaResponse.FileLength,
				JPEGThumbnail: thumbnail,
			}
		default:
			return nil, fmt.Errorf("unsupported media type: %v", req.Media.Type)
		}
	}

	extra := whatsmeow.SendRequestExtra{}
	if mediaResponse.Handle != "" {
		// Newsletters
		extra.MediaHandle = mediaResponse.Handle
	}

	sendCtx, cancel := withOperationTimeout(ctx, sendTimeout)
	defer cancel()
	res, err := cli.SendMessage(sendCtx, jid, &message, extra)
	if err != nil {
		return nil, fmt.Errorf("send message: %w", err)
	}

	return &__.MessageResponse{Id: res.ID, Timestamp: res.Timestamp.Unix()}, nil
}

func timeoutFromEnv(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	if value, err := time.ParseDuration(raw); err == nil && value > 0 {
		return value
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func withOperationTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok {
		if time.Until(deadline) <= timeout {
			return context.WithCancel(parent)
		}
	}
	return context.WithTimeout(parent, timeout)
}

func runBytesWithTimeout(timeout time.Duration, fn func() ([]byte, error)) ([]byte, error) {
	type output struct {
		data []byte
		err  error
	}
	done := make(chan output, 1)
	go func() {
		data, err := fn()
		done <- output{data: data, err: err}
	}()
	select {
	case result := <-done:
		return result.data, result.err
	case <-time.After(timeout):
		return nil, fmt.Errorf("operation timed out after %s", timeout)
	}
}

func (s *Server) SendReaction(ctx context.Context, req *__.MessageReaction) (*__.MessageResponse, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.Jid)
	sender, err := types.ParseJID(req.Sender)

	message := cli.BuildReaction(jid, sender, req.MessageId, req.Reaction)
	res, err := cli.SendMessage(ctx, jid, message)
	if err != nil {
		return nil, err
	}

	return &__.MessageResponse{Id: res.ID, Timestamp: res.Timestamp.Unix()}, nil
}

func (s *Server) MarkRead(ctx context.Context, req *__.MarkReadRequest) (*__.Empty, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	jid, err := types.ParseJID(req.Jid)
	if err != nil {
		return nil, err
	}

	sender, err := types.ParseJID(req.Sender)
	if err != nil {
		return nil, err
	}

	var receiptType types.ReceiptType
	switch req.Type {
	case __.ReceiptType_READ:
		receiptType = types.ReceiptTypeRead
	case __.ReceiptType_PLAYED:
		receiptType = types.ReceiptTypePlayed
	default:
		return nil, errors.New("invalid receipt type: " + req.Type.String())
	}

	// id to ids array
	ids := []types.MessageID{req.MessageId}
	now := time.Now()
	err = cli.MarkRead(ctx, ids, now, jid, sender, receiptType)
	if err != nil {
		return nil, err
	}
	return &__.Empty{}, nil
}
