package crawlserviceclient

import (
	"context"
	"encoding/json"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/nats-io/nats.go"
)

const DefaultBrregDomainDiscoverySubject = "brreg.domain.discover"

type natsRequester interface {
	Request(context.Context, string, []byte) ([]byte, error)
}

type NATSClient struct {
	subject   string
	requester natsRequester
	conn      *nats.Conn
}

func NewNATS(url string) (*NATSClient, error) {
	return NewNATSWithSubject(url, DefaultBrregDomainDiscoverySubject)
}

func NewNATSWithSubject(url string, subject string) (*NATSClient, error) {
	if subject == "" {
		subject = DefaultBrregDomainDiscoverySubject
	}
	conn, err := nats.Connect(url, nats.Timeout(10*time.Second))
	if err != nil {
		return nil, errors.Wrap(err, "connect to nats")
	}
	return &NATSClient{
		subject:   subject,
		requester: natsConnRequester{conn: conn},
		conn:      conn,
	}, nil
}

func newNATSClientFromRequester(subject string, requester natsRequester) *NATSClient {
	if subject == "" {
		subject = DefaultBrregDomainDiscoverySubject
	}
	return &NATSClient{subject: subject, requester: requester}
}

func (c *NATSClient) DiscoverBrregDomain(ctx context.Context, request BrregDomainDiscoveryRequest) (BrregDomainDiscoveryResponse, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return BrregDomainDiscoveryResponse{}, errors.Wrap(err, "encode brreg domain discovery nats request")
	}
	responsePayload, err := c.requester.Request(ctx, c.subject, payload)
	if err != nil {
		return BrregDomainDiscoveryResponse{}, errors.Wrap(err, "request brreg domain discovery over nats")
	}
	var decoded BrregDomainDiscoveryResponse
	if err := json.Unmarshal(responsePayload, &decoded); err != nil {
		return BrregDomainDiscoveryResponse{}, errors.Wrap(err, "decode brreg domain discovery nats response")
	}
	decoded.RawPayload = json.RawMessage(responsePayload)
	return decoded, nil
}

func (c *NATSClient) Close() {
	if c != nil && c.conn != nil {
		c.conn.Close()
	}
}

type natsConnRequester struct {
	conn *nats.Conn
}

func (r natsConnRequester) Request(ctx context.Context, subject string, payload []byte) ([]byte, error) {
	msg, err := r.conn.RequestWithContext(ctx, subject, payload)
	if err != nil {
		return nil, err
	}
	return msg.Data, nil
}
