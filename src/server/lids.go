package server

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/devlikeapro/gows/proto"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

type lidMappingsLister interface {
	GetAllLIDMappings(ctx context.Context) ([]store.LIDMapping, error)
}

type lidMappingsCounter interface {
	GetLIDMappingsCount(ctx context.Context) (uint64, error)
}

func (s *Server) GetAllLids(ctx context.Context, req *__.GetLidsRequest) (*__.JsonList, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	list, ok := cli.Store.LIDs.(lidMappingsLister)
	if !ok {
		// Store backend doesn't expose list operation.
		return &__.JsonList{}, nil
	}

	mappings, err := list.GetAllLIDMappings(ctx)
	if err != nil {
		return nil, err
	}

	elements := make([]*__.Json, 0, len(mappings))
	for _, mapping := range mappings {
		payload := map[string]string{
			"lid": mapping.LID.ToNonAD().String(),
			"pn":  mapping.PN.ToNonAD().String(),
		}
		data, marshalErr := json.Marshal(payload)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal lid mapping: %w", marshalErr)
		}
		elements = append(elements, &__.Json{Data: string(data)})
	}
	return &__.JsonList{Elements: elements}, nil
}

func (s *Server) GetLidsCount(ctx context.Context, req *__.Session) (*__.OptionalUInt64, error) {
	cli, err := s.Sm.Get(req.GetId())
	if err != nil {
		return nil, err
	}

	counter, ok := cli.Store.LIDs.(lidMappingsCounter)
	if !ok {
		// Store backend doesn't expose count operation.
		return &__.OptionalUInt64{}, nil
	}

	count, err := counter.GetLIDMappingsCount(ctx)
	if err != nil {
		return nil, err
	}
	return &__.OptionalUInt64{Value: count}, nil
}

func (s *Server) FindPNByLid(ctx context.Context, req *__.EntityByIdRequest) (*__.OptionalString, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	lid, err := types.ParseJID(req.GetId())
	if err != nil {
		return nil, err
	}

	pn, err := cli.Store.LIDs.GetPNForLID(ctx, lid)
	if err != nil {
		return nil, err
	}

	if pn.User == "" {
		return &__.OptionalString{}, nil
	}
	return &__.OptionalString{Value: pn.ToNonAD().String()}, nil
}

func (s *Server) FindLIDByPhoneNumber(ctx context.Context, req *__.EntityByIdRequest) (*__.OptionalString, error) {
	cli, err := s.Sm.Get(req.GetSession().GetId())
	if err != nil {
		return nil, err
	}

	pn, err := types.ParseJID(req.GetId())
	if err != nil {
		return nil, err
	}

	lid, err := cli.Store.LIDs.GetLIDForPN(ctx, pn)
	if err != nil {
		return nil, err
	}

	if lid.User == "" {
		return &__.OptionalString{}, nil
	}
	return &__.OptionalString{Value: lid.ToNonAD().String()}, nil
}

