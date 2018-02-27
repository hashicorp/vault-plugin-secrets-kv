package vkv

import (
	"context"
	"strings"

	"github.com/hashicorp/vault/logical"
	"github.com/hashicorp/vault/logical/framework"
)

const (
	kvClientHeader string = "X-Vault-KV-Client"
)

type PassthroughDowngrader struct {
	next Passthrough
}

func (b *PassthroughDowngrader) handleExistenceCheck() framework.ExistenceFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (bool, error) {
		if !b.shouldDowngrade(req) {
			return b.next.handleExistenceCheck()(ctx, req, data)
		}

		respErr := b.invalidPath(req)
		if respErr != nil {
			return false, logical.ErrInvalidRequest
		}

		var down *logical.Request
		*down = *req

		down.Path = strings.TrimPrefix(req.Path, "data/")
		return b.next.handleExistenceCheck()(ctx, req, data)
	}
}

func (b *PassthroughDowngrader) handleRead() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		if !b.shouldDowngrade(req) {
			return b.next.handleRead()(ctx, req, data)
		}

		respErr := b.invalidPath(req)
		if respErr != nil {
			return respErr, logical.ErrInvalidRequest
		}

		if _, ok := data.Raw["version"]; ok {
			return logical.ErrorResponse("retrieving a version is not supported when versioning is disabled"), logical.ErrInvalidRequest
		}

		var down *logical.Request
		*down = *req

		down.Path = strings.TrimPrefix(req.Path, "data/")

		// TODO: should we upgrade the response?
		resp, err := b.next.handleRead()(ctx, down, data)
		if resp != nil && resp.Data != nil {
			resp.Data = map[string]interface{}{
				"data":     resp.Data,
				"metadata": nil,
			}
		}

		return resp, err
	}
}

func (b *PassthroughDowngrader) handleWrite() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		if !b.shouldDowngrade(req) {
			return b.next.handleWrite()(ctx, req, data)
		}

		respErr := b.invalidPath(req)
		if respErr != nil {
			return respErr, logical.ErrInvalidRequest
		}

		var reqDown *logical.Request
		*reqDown = *req
		reqDown.Path = strings.TrimPrefix(req.Path, "data/")

		// Validate the data map is what we expect
		switch data.Raw["data"].(type) {
		case map[string]interface{}:
		default:
			return logical.ErrorResponse("Could not downgrade request, unexpected data format"), logical.ErrInvalidRequest
		}

		dataDown := &framework.FieldData{
			Raw: data.Raw["data"].(map[string]interface{}),
		}

		return b.next.handleWrite()(ctx, reqDown, dataDown)
	}
}

func (b *PassthroughDowngrader) handleDelete() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		if !b.shouldDowngrade(req) {
			return b.next.handleDelete()(ctx, req, data)
		}

		respErr := b.invalidPath(req)
		if respErr != nil {
			return respErr, logical.ErrInvalidRequest
		}

		var reqDown *logical.Request
		*reqDown = *req
		reqDown.Path = strings.TrimPrefix(req.Path, "data/")

		return b.next.handleDelete()(ctx, reqDown, data)
	}
}

func (b *PassthroughDowngrader) handleList() framework.OperationFunc {
	return func(ctx context.Context, req *logical.Request, data *framework.FieldData) (*logical.Response, error) {
		if !b.shouldDowngrade(req) {
			return b.next.handleList()(ctx, req, data)
		}

		respErr := b.invalidPath(req)
		if respErr != nil {
			return respErr, logical.ErrInvalidRequest
		}

		var reqDown *logical.Request
		*reqDown = *req
		reqDown.Path = strings.TrimPrefix(req.Path, "data/")

		return b.next.handleList()(ctx, reqDown, data)
	}
}

func (b *PassthroughDowngrader) shouldDowngrade(req *logical.Request) bool {
	_, ok := req.Headers[kvClientHeader]
	return ok
}

func (b *PassthroughDowngrader) invalidPath(req *logical.Request) *logical.Response {
	switch {
	case strings.HasPrefix(req.Path, "metadata/"):
		fallthrough
	case strings.HasPrefix(req.Path, "archive/"):
		fallthrough
	case strings.HasPrefix(req.Path, "unarchive/"):
		fallthrough
	case strings.HasPrefix(req.Path, "destroy/"):
		return logical.ErrorResponse("path is not supported when versioning is disabled")
	}

	return nil
}
