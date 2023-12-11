package dtable

import (
	"bytes"
	"context"
	sqldrv "database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type client struct {
	baseId  string
	baseUrl url.URL
	token   string
}

var ErrConflict = errors.New("transaction conflict, please retry")

func newClient(baseId string, baseUrl *url.URL, privateKey string) (*client, error) {
	token, err := newDtableJWT(privateKey, baseId)
	if err != nil {
		return nil, err
	}

	cli := &client{
		baseId:  baseId,
		baseUrl: *baseUrl,
		token:   token,
	}
	return cli, nil
}

func (c *client) makeRequest(ctx context.Context, method string, path string, reader io.Reader) (*http.Request, error) {
	u := c.baseUrl
	u.Path += path
	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return nil, err
	}
	req.Header = map[string][]string{
		"Authorization": {"Token " + c.token},
		"Accept":        {"application/json"},
		"Content-Type":  {"application/json"},
		"charset":       {"utf-8"},
	}
	return req, nil
}

type beginTxnResult struct {
	TxnId string `json:"txn_id"`
}

func (c *client) beginTxn(ctx context.Context) (string, error) {
	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/txn/%s/begin", c.baseId), nil)
	if err != nil {
		return "", err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	if rsp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad response for beginTxn : %d", rsp.StatusCode)
	}

	data, err := io.ReadAll(rsp.Body)
	if err != nil {
		return "", err
	}

	var res beginTxnResult
	err = json.Unmarshal(data, &res)
	if err != nil {
		return "", err
	}
	return res.TxnId, nil
}

func (c *client) execTxn(ctx context.Context, txnId string, query string, param []sqldrv.Value) error {
	data, err := json.Marshal(map[string]any{
		"sql":        query,
		"parameters": param,
	})
	if err != nil {
		return err
	}

	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/txn/%s/exec", txnId), bytes.NewReader(data))
	if err != nil {
		return err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusOK {
		bts, err := io.ReadAll(rsp.Body)
		if err != nil {
			return fmt.Errorf("bad response for execTxn: %d", rsp.StatusCode)
		}
		var res queryResponse
		_ = json.Unmarshal(bts, &res)
		//txn is too big
		if res.ErrorMsg == "please retry" {
			return ErrConflict
		}
		return fmt.Errorf("bad response for execTxn: %d, %s", rsp.StatusCode, res.ErrorMsg)
	}
	return nil
}

func (c *client) queryTxn(ctx context.Context, txnId string, query string, param []sqldrv.Value) (*dtableRows, error) {
	data, err := json.Marshal(map[string]any{
		"sql":        query,
		"parameters": param,
	})
	if err != nil {
		return nil, err
	}

	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/txn/%s/exec", txnId), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode != http.StatusOK {
		bts, err := io.ReadAll(rsp.Body)
		if err != nil {
			return nil, fmt.Errorf("bad response for queryTxn: %d", rsp.StatusCode)
		}
		var res queryResponse
		_ = json.Unmarshal(bts, &res)
		return nil, fmt.Errorf("bad response for queryTxn: %d, %s", rsp.StatusCode, res.ErrorMsg)
	}

	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	var result dtableRows
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

func (c *client) commitTxn(ctx context.Context, txnId string) error {
	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/txn/%s/commit", txnId), nil)
	if err != nil {
		return err
	}

	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusOK {
		bts, err := io.ReadAll(rsp.Body)
		if err != nil {
			return fmt.Errorf("bad response for commitTxn: %d", rsp.StatusCode)
		}
		var res queryResponse
		_ = json.Unmarshal(bts, &res)
		if res.ErrorMsg == "please retry" {
			return ErrConflict
		}
		return fmt.Errorf("bad response for commitTxn : %d", rsp.StatusCode)
	}
	return nil
}

func (c *client) rollBackTxn(ctx context.Context, txnId string) error {
	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/txn/%s/rollback", txnId), nil)
	if err != nil {
		return err
	}

	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusOK {
		return fmt.Errorf("bad response for commitTxn : %d", rsp.StatusCode)
	}
	return nil
}

func (c *client) query(ctx context.Context, query string, param []sqldrv.Value) (*dtableRows, error) {
	data, err := json.Marshal(map[string]any{
		"sql":        query,
		"parameters": param,
	})
	if err != nil {
		return nil, err
	}

	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/query/%s", c.baseId), bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad response for query : %d", rsp.StatusCode)
	}

	body, err := io.ReadAll(rsp.Body)
	if err != nil {
		return nil, err
	}
	var result dtableRows
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

type queryResponse struct {
	ErrorMsg string `json:"error_message"`
}

func (c *client) exec(ctx context.Context, query string, param []sqldrv.Value) error {
	data, err := json.Marshal(map[string]any{
		"sql":        query,
		"parameters": param,
	})
	if err != nil {
		return err
	}

	req, err := c.makeRequest(ctx, "POST", fmt.Sprintf("api/v1/query/%s", c.baseId), bytes.NewReader(data))
	if err != nil {
		return err
	}
	rsp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if rsp.StatusCode != http.StatusOK {
		bts, err := io.ReadAll(rsp.Body)
		if err != nil {
			return fmt.Errorf("bad response for exec: %d", rsp.StatusCode)
		}
		var res queryResponse
		_ = json.Unmarshal(bts, &res)
		return fmt.Errorf("bad response for exec: %d, %s", rsp.StatusCode, res.ErrorMsg)
	}
	return nil
}
