package client

import (
	"net/url"

	"github.com/block/schemabot/pkg/apitypes"
)

// CreateFreeze sets an environment freeze.
func CreateFreeze(endpoint string, req apitypes.CreateFreezeRequest) (*apitypes.Freeze, error) {
	var result apitypes.Freeze
	if err := doPostInto(endpoint, "/api/freezes", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListFreezes lists the freezes in effect, the scheduled ones and the recent
// ones. An empty environment lists every environment.
func ListFreezes(endpoint, environment string) (*apitypes.FreezeListResponse, error) {
	path := "/api/freezes"
	if environment != "" {
		path += "?" + url.Values{"environment": {environment}}.Encode()
	}
	var result apitypes.FreezeListResponse
	if err := doGetInto(endpoint, path, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFreeze fetches one freeze.
func GetFreeze(endpoint, id string) (*apitypes.Freeze, error) {
	var result apitypes.Freeze
	if err := doGetInto(endpoint, "/api/freezes/"+url.PathEscape(id), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// LiftFreeze ends a freeze early.
func LiftFreeze(endpoint, id, reason string) (*apitypes.Freeze, error) {
	var result apitypes.Freeze
	err := doPostInto(endpoint, "/api/freezes/"+url.PathEscape(id)+"/lift", apitypes.LiftFreezeRequest{Reason: reason}, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// GetFreezeOverride fetches one freeze override request.
func GetFreezeOverride(endpoint, id string) (*apitypes.FreezeOverrideRequest, error) {
	var result apitypes.FreezeOverrideRequest
	if err := doGetInto(endpoint, "/api/freezes/overrides/"+url.PathEscape(id), &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ApproveFreezeOverride approves a freeze override request. The server takes
// the approver from the caller's verified identity, so the body is empty.
func ApproveFreezeOverride(endpoint, id string) (*apitypes.FreezeOverrideRequest, error) {
	var result apitypes.FreezeOverrideRequest
	err := doPostInto(endpoint, "/api/freezes/overrides/"+url.PathEscape(id)+"/approve", struct{}{}, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}
