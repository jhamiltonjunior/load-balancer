package main

import (
	"context"

	"github.com/valyala/fasthttp"
)

func RequestWithoutResponse(method string, json []byte, reqURL string, ctx context.Context) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqURL)
	req.Header.SetMethod(method)
	req.Header.Set("Content-Type", "application/json")
	if json != nil {
		req.SetBody(json)
	}

	go func() {
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)
		_ = fasthttp.Do(req, resp)
	}()
}

func RequestWithResponse(method string, json []byte, reqURL string, ctx *fasthttp.RequestCtx) {
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(reqURL)
	req.Header.SetMethod(method)
	req.Header.Set("Content-Type", "application/json")
	if json != nil {
		req.SetBody(json)
	}

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := fasthttp.Do(req, resp); err != nil {
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(resp.StatusCode())
	ctx.SetContentType("application/json")
	ctx.Write(resp.Body())
}
