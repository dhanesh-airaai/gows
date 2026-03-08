package server

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/devlikeapro/gows/gows"
	"github.com/devlikeapro/gows/proto"
	"github.com/google/uuid"
	"go.mau.fi/whatsmeow/types"
)

func jsonData(v interface{}) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "{}"
	}
	return string(b)
}

func emptyJsonList() *__.JsonList {
	return &__.JsonList{Elements: []*__.Json{}}
}

func (s *Server) SetProfileName(ctx context.Context, req *__.ProfileNameRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) SetProfileStatus(ctx context.Context, req *__.ProfileStatusRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) SetProfilePicture(ctx context.Context, req *__.SetProfilePictureRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) FetchGroups(ctx context.Context, req *__.Session) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) GetGroups(ctx context.Context, req *__.Session) (*__.JsonList, error) {
	return emptyJsonList(), nil
}

func (s *Server) GetGroupInfo(ctx context.Context, req *__.JidRequest) (*__.Json, error) {
	jid, err := types.ParseJID(req.GetJid())
	if err != nil {
		return nil, err
	}
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}
	info, err := cli.GetGroupInfo(ctx, jid)
	if err != nil {
		return nil, err
	}
	return &__.Json{Data: jsonData(info)}, nil
}

func (s *Server) CreateGroup(ctx context.Context, req *__.CreateGroupRequest) (*__.Json, error) {
	return &__.Json{Data: "{}"}, nil
}

func (s *Server) LeaveGroup(ctx context.Context, req *__.JidRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) GetGroupInviteLink(ctx context.Context, req *__.JidRequest) (*__.OptionalString, error) {
	return &__.OptionalString{}, nil
}

func (s *Server) RevokeGroupInviteLink(ctx context.Context, req *__.JidRequest) (*__.OptionalString, error) {
	return &__.OptionalString{}, nil
}

func (s *Server) GetGroupInfoFromLink(ctx context.Context, req *__.GroupCodeRequest) (*__.Json, error) {
	return &__.Json{Data: "{}"}, nil
}

func (s *Server) JoinGroupWithLink(ctx context.Context, req *__.GroupCodeRequest) (*__.Json, error) {
	return &__.Json{Data: "{}"}, nil
}

func (s *Server) SetGroupName(ctx context.Context, req *__.JidStringRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) SetGroupDescription(ctx context.Context, req *__.JidStringRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) SetGroupPicture(ctx context.Context, req *__.SetPictureRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) SetGroupLocked(ctx context.Context, req *__.JidBoolRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) SetGroupAnnounce(ctx context.Context, req *__.JidBoolRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) UpdateGroupParticipants(ctx context.Context, req *__.UpdateParticipantsRequest) (*__.JsonList, error) {
	return emptyJsonList(), nil
}

func (s *Server) MarkChatUnread(ctx context.Context, req *__.ChatUnreadRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) GenerateNewMessageID(ctx context.Context, req *__.Session) (*__.NewMessageIDResponse, error) {
	return &__.NewMessageIDResponse{Id: uuid.NewString()}, nil
}

func (s *Server) EditMessage(ctx context.Context, req *__.EditMessageRequest) (*__.MessageResponse, error) {
	return &__.MessageResponse{Id: req.GetMessageId(), Timestamp: time.Now().Unix()}, nil
}

func (s *Server) RevokeMessage(ctx context.Context, req *__.RevokeMessageRequest) (*__.MessageResponse, error) {
	return &__.MessageResponse{Id: req.GetMessageId(), Timestamp: time.Now().Unix()}, nil
}

func (s *Server) SendButtonReply(ctx context.Context, req *__.ButtonReplyRequest) (*__.MessageResponse, error) {
	return &__.MessageResponse{Id: uuid.NewString(), Timestamp: time.Now().Unix()}, nil
}

func (s *Server) GetNewsletterMessagesByInvite(ctx context.Context, req *__.GetNewsletterMessagesByInviteRequest) (*__.Json, error) {
	return &__.Json{Data: "{}"}, nil
}

func (s *Server) SearchNewslettersByView(ctx context.Context, req *__.SearchNewslettersByViewRequest) (*__.NewsletterSearchPageResult, error) {
	return &__.NewsletterSearchPageResult{
		Page: &__.SearchPageResult{},
		Newsletters: &__.NewsletterList{
			Newsletters: []*__.Newsletter{},
		},
	}, nil
}

func (s *Server) SearchNewslettersByText(ctx context.Context, req *__.SearchNewslettersByTextRequest) (*__.NewsletterSearchPageResult, error) {
	return &__.NewsletterSearchPageResult{
		Page: &__.SearchPageResult{},
		Newsletters: &__.NewsletterList{
			Newsletters: []*__.Newsletter{},
		},
	}, nil
}

func (s *Server) GetLabels(ctx context.Context, req *__.GetLabelsRequest) (*__.JsonList, error) {
	return emptyJsonList(), nil
}

func (s *Server) UpsertLabel(ctx context.Context, req *__.UpsertLabelRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) DeleteLabel(ctx context.Context, req *__.DeleteLabelRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) AddChatLabel(ctx context.Context, req *__.ChatLabelRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) RemoveChatLabel(ctx context.Context, req *__.ChatLabelRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) GetLabelsByJid(ctx context.Context, req *__.EntityByIdRequest) (*__.JsonList, error) {
	return emptyJsonList(), nil
}

func (s *Server) GetChatsByLabelId(ctx context.Context, req *__.EntityByIdRequest) (*__.JsonList, error) {
	return emptyJsonList(), nil
}

func (s *Server) UpdateContact(ctx context.Context, req *__.UpdateContactRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) GetContacts(ctx context.Context, req *__.GetContactsRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	contacts, err := cli.Store.Contacts.GetAllContacts(ctx)
	if err != nil {
		return nil, err
	}

	type contactItem struct {
		Jid      string `json:"Jid"`
		Name     string `json:"Name"`
		PushName string `json:"PushName"`
	}

	items := make([]contactItem, 0, len(contacts))
	for jid, info := range contacts {
		normalized := jid.ToNonAD().String()
		name := info.FullName
		if name == "" {
			name = info.BusinessName
		}
		items = append(items, contactItem{
			Jid:      normalized,
			Name:     name,
			PushName: info.PushName,
		})
	}

	// Sorting
	sortField := "id"
	sortDesc := false
	if req.GetSortBy() != nil {
		if req.GetSortBy().GetField() != "" {
			sortField = strings.ToLower(req.GetSortBy().GetField())
		}
		sortDesc = req.GetSortBy().GetOrder() == __.SortBy_DESC
	}
	sort.Slice(items, func(i, j int) bool {
		var less bool
		switch sortField {
		case "name":
			if items[i].Name == items[j].Name {
				less = items[i].Jid < items[j].Jid
			} else {
				less = items[i].Name < items[j].Name
			}
		default:
			less = items[i].Jid < items[j].Jid
		}
		if sortDesc {
			return !less
		}
		return less
	})

	// Pagination
	offset := 0
	limit := len(items)
	if req.GetPagination() != nil {
		if req.GetPagination().GetOffset() > 0 {
			offset = int(req.GetPagination().GetOffset())
		}
		if req.GetPagination().GetLimit() > 0 {
			limit = int(req.GetPagination().GetLimit())
		}
	}
	if offset > len(items) {
		offset = len(items)
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	items = items[offset:end]

	elements := make([]*__.Json, 0, len(items))
	for _, item := range items {
		elements = append(elements, &__.Json{Data: jsonData(item)})
	}
	return &__.JsonList{Elements: elements}, nil
}

func (s *Server) GetContactById(ctx context.Context, req *__.EntityByIdRequest) (*__.Json, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	jid, err := types.ParseJID(req.GetId())
	if err != nil {
		return &__.Json{Data: "{}"}, nil
	}

	info, err := cli.Store.Contacts.GetContact(ctx, jid.ToNonAD())
	if err != nil || !info.Found {
		return &__.Json{Data: "{}"}, nil
	}

	name := info.FullName
	if name == "" {
		name = info.BusinessName
	}
	payload := map[string]interface{}{
		"Jid":      jid.ToNonAD().String(),
		"Name":     name,
		"PushName": info.PushName,
	}
	return &__.Json{Data: jsonData(payload)}, nil
}

func (s *Server) CancelEventMessage(ctx context.Context, req *__.CancelEventMessageRequest) (*__.MessageResponse, error) {
	return &__.MessageResponse{Id: req.GetMessageId(), Timestamp: time.Now().Unix()}, nil
}

func (s *Server) RejectCall(ctx context.Context, req *__.RejectCallRequest) (*__.Empty, error) {
	return &__.Empty{}, nil
}

func (s *Server) GetMessageById(ctx context.Context, req *__.EntityByIdRequest) (*__.Json, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	type messageEventByIDProvider interface {
		GetMessageEventByID(context.Context, string) (string, error)
	}

	provider, ok := cli.Store.LIDs.(messageEventByIDProvider)
	if !ok {
		return &__.Json{Data: "null"}, nil
	}

	data, err := provider.GetMessageEventByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(data) == "" {
		return &__.Json{Data: "null"}, nil
	}
	return &__.Json{Data: data}, nil
}

func (s *Server) GetMessages(ctx context.Context, req *__.GetMessagesRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	type messageEventsProvider interface {
		ListMessageEvents(
			context.Context,
			[]string,
			*int64,
			*int64,
			*bool,
			*uint32,
			string,
			bool,
			int,
			int,
		) ([]string, error)
	}

	provider, ok := cli.Store.LIDs.(messageEventsProvider)
	if !ok {
		return emptyJsonList(), nil
	}

	var chatCandidates []string
	var timestampGteMS *int64
	var timestampLteMS *int64
	var fromMe *bool
	var status *uint32

	if filters := req.GetFilters(); filters != nil {
		if jidFilter := filters.GetJid(); jidFilter != nil {
			jid := strings.TrimSpace(jidFilter.GetValue())
			chatCandidates = s.resolveMessageChatCandidates(ctx, cli, jid)
		}
		if filters.GetTimestampGte() != nil {
			ts := normalizeToMillis(int64(filters.GetTimestampGte().GetValue()))
			timestampGteMS = &ts
		}
		if filters.GetTimestampLte() != nil {
			ts := normalizeToMillis(int64(filters.GetTimestampLte().GetValue()))
			timestampLteMS = &ts
		}
		if filters.GetFromMe() != nil {
			value := filters.GetFromMe().GetValue()
			fromMe = &value
		}
		if filters.GetStatus() != nil {
			value := filters.GetStatus().GetValue()
			status = &value
		}
	}

	sortField := "timestamp"
	sortDesc := true
	if req.GetSortBy() != nil {
		field := strings.TrimSpace(strings.ToLower(req.GetSortBy().GetField()))
		if field != "" {
			sortField = field
		}
		sortDesc = req.GetSortBy().GetOrder() == __.SortBy_DESC
	}

	offset := 0
	limit := 100
	if req.GetPagination() != nil {
		offset = int(req.GetPagination().GetOffset())
		limit = int(req.GetPagination().GetLimit())
	}
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}

	rows, err := provider.ListMessageEvents(
		ctx,
		chatCandidates,
		timestampGteMS,
		timestampLteMS,
		fromMe,
		status,
		sortField,
		sortDesc,
		offset,
		limit,
	)
	if err != nil {
		return nil, err
	}

	elements := make([]*__.Json, 0, len(rows))
	for _, row := range rows {
		if strings.TrimSpace(row) == "" {
			continue
		}
		elements = append(elements, &__.Json{Data: row})
	}
	return &__.JsonList{Elements: elements}, nil
}

func normalizeToMillis(value int64) int64 {
	if value <= 0 {
		return value
	}
	// Accept both seconds and milliseconds from API filters.
	if value < 1_000_000_000_000 {
		return value * 1000
	}
	return value
}

func (s *Server) resolveMessageChatCandidates(ctx context.Context, cli *gows.GoWS, jid string) []string {
	jid = strings.TrimSpace(jid)
	if jid == "" {
		return nil
	}

	jid = strings.ReplaceAll(jid, "@c.us", "@s.whatsapp.net")
	if !strings.Contains(jid, "@") {
		jid = jid + "@s.whatsapp.net"
	}

	parsed, err := types.ParseJID(jid)
	if err != nil {
		return []string{jid}
	}
	parsed = parsed.ToNonAD()

	candidates := map[string]struct{}{
		parsed.String(): {},
	}

	switch parsed.Server {
	case types.DefaultUserServer:
		if lid, lidErr := cli.Store.LIDs.GetLIDForPN(ctx, parsed); lidErr == nil && lid.User != "" {
			candidates[lid.ToNonAD().String()] = struct{}{}
		}
	case types.HiddenUserServer:
		if pn, pnErr := cli.Store.LIDs.GetPNForLID(ctx, parsed); pnErr == nil && pn.User != "" {
			candidates[pn.ToNonAD().String()] = struct{}{}
		}
	}

	result := make([]string, 0, len(candidates))
	for candidate := range candidates {
		result = append(result, candidate)
	}
	return result
}

func (s *Server) GetChats(ctx context.Context, req *__.GetChatsRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	type recentChatsProvider interface {
		GetRecentChatTimestamps(context.Context) (map[string]int64, error)
	}

	type chatItem struct {
		Jid       string
		Name      string
		Timestamp int64
	}

	contactNames := make(map[string]string)
	itemsByJid := make(map[string]*chatItem)

	// Contacts-backed names for chat display.
	contacts, err := cli.Store.Contacts.GetAllContacts(ctx)
	if err == nil {
		for jid, info := range contacts {
			normalized := jid.ToNonAD().String()
			name := info.FullName
			if name == "" {
				name = info.BusinessName
			}
			if name == "" {
				name = info.PushName
			}
			if name == "" {
				name = info.FirstName
			}
			contactNames[normalized] = name
		}
	}

	// Add recent chats inferred from stored message timestamps if available.
	recent := map[string]int64{}
	if provider, ok := cli.Store.LIDs.(recentChatsProvider); ok {
		recent, _ = provider.GetRecentChatTimestamps(ctx)
	}

	if len(recent) > 0 {
		for jid, ts := range recent {
			jidObj, parseErr := types.ParseJID(jid)
			if parseErr != nil {
				continue
			}
			normalized := jidObj.ToNonAD().String()
			itemsByJid[normalized] = &chatItem{
				Jid:       normalized,
				Name:      contactNames[normalized],
				Timestamp: ts,
			}
		}
	} else {
		// If no recent message data exists yet, fall back to known contacts.
		for normalized, name := range contactNames {
			itemsByJid[normalized] = &chatItem{
				Jid:       normalized,
				Name:      name,
				Timestamp: 0,
			}
		}
	}

	items := make([]chatItem, 0, len(itemsByJid))
	for _, item := range itemsByJid {
		items = append(items, *item)
	}

	// Optional filter by specific JIDs.
	filterMap := map[string]struct{}{}
	if req.GetFilter() != nil {
		for _, jid := range req.GetFilter().GetJids() {
			jidObj, parseErr := types.ParseJID(jid)
			if parseErr != nil {
				continue
			}
			filterMap[jidObj.ToNonAD().String()] = struct{}{}
		}
	}
	if len(filterMap) > 0 {
		filtered := make([]chatItem, 0, len(items))
		for _, item := range items {
			if _, ok := filterMap[item.Jid]; ok {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}

	// Sorting
	sortField := "id"
	sortDesc := false
	if req.GetSortBy() != nil {
		if req.GetSortBy().GetField() != "" {
			sortField = strings.ToLower(req.GetSortBy().GetField())
		}
		sortDesc = req.GetSortBy().GetOrder() == __.SortBy_DESC
	}
	sort.Slice(items, func(i, j int) bool {
		var less bool
		switch sortField {
		case "timestamp", "conversationtimestamp":
			if items[i].Timestamp == items[j].Timestamp {
				less = items[i].Jid < items[j].Jid
			} else {
				less = items[i].Timestamp < items[j].Timestamp
			}
		case "name":
			if items[i].Name == items[j].Name {
				less = items[i].Jid < items[j].Jid
			} else {
				less = items[i].Name < items[j].Name
			}
		default:
			less = items[i].Jid < items[j].Jid
		}
		if sortDesc {
			return !less
		}
		return less
	})

	// Pagination
	offset := 0
	limit := len(items)
	if req.GetPagination() != nil {
		offset = int(req.GetPagination().GetOffset())
		limit = int(req.GetPagination().GetLimit())
	}
	if offset > len(items) {
		offset = len(items)
	}
	if limit <= 0 {
		limit = len(items)
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	items = items[offset:end]

	// Transform to WAHA expected JSON shape.
	elements := make([]*__.Json, 0, len(items))
	for _, item := range items {
		var tsValue interface{}
		if item.Timestamp > 0 {
			tsValue = time.UnixMilli(item.Timestamp).UTC().Format(time.RFC3339)
		}
		payload := map[string]interface{}{
			"Jid":                   item.Jid,
			"Name":                  item.Name,
			"ConversationTimestamp": tsValue,
		}
		elements = append(elements, &__.Json{
			Data: jsonData(payload),
		})
	}
	return &__.JsonList{Elements: elements}, nil
}
