package live

import (
	"fmt"
	"sync"
	"time"

	"github.com/centrifugal/centrifuge-go"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
)

// Client communicates with the GrafanaLive server.
type Client struct {
	connected bool
	client    *centrifuge.Client
	lastWarn  time.Time
	channels  map[string]*Channel
	log       log.Logger
}

// Channel allows access to a channel within the server.
type Channel struct {
	id           string
	client       *Client
	subscription *Subscription
	subscribed   bool
}

// NewChannel creates new Channel.
func (c *Client) NewChannel(addr ChannelAddress) (*Channel, error) {
	id := addr.ToChannelID()
	if !addr.IsValid() {
		return nil, fmt.Errorf("invalid channel: %s", id)
	}
	ch := &Channel{
		id:     id,
		client: c,
	}
	return ch, nil
}

// Close live client.
func (c *Client) Close() error {
	return c.client.Close()
}

// Publish sends the data to the channel.
func (c *Channel) Publish(data []byte) error {
	if !c.client.connected {
		if time.Since(c.client.lastWarn) > time.Second*5 {
			c.client.lastWarn = time.Now()
			c.client.log.Warn("Grafana live channel not connected", "id", c.id)
		}
		return fmt.Errorf("client not connected")
	}
	_, err := c.client.client.Publish(c.id, data)
	if err != nil {
		c.client.log.Info("error publishing", "error", err)
		return fmt.Errorf("error publishing: %w", err)
	}
	return nil
}

// Message from a channel.
type Message struct {
	// Data contains message payload.
	Data []byte
}

// MessageHandler allows handling messages coming from a channel.
type MessageHandler func(*Message) error

// Subscription to a channel to receive updates from it.
type Subscription struct {
	sub            *centrifuge.Subscription
	channelHandler *liveChannelHandler
}

// Close subscription.
func (s *Subscription) Close() error {
	s.channelHandler.Close()
	err := s.sub.Close()
	if err != nil {
		return err
	}
	return nil
}

// Subscribe to the live channel to receive updates from it.
func (c *Channel) Subscribe(msgHandler MessageHandler) (*Subscription, error) {
	sub, err := c.client.client.NewSubscription(c.id)
	if err != nil {
		return nil, err
	}

	channelHandler := &liveChannelHandler{channel: c, msgHandler: msgHandler}

	sub.OnSubscribeSuccess(channelHandler)
	sub.OnSubscribeError(channelHandler)
	sub.OnUnsubscribe(channelHandler)
	sub.OnPublish(channelHandler)

	err = sub.Subscribe()
	if err != nil {
		return nil, err
	}

	return &Subscription{sub: sub, channelHandler: channelHandler}, nil
}

// Close live channel.
func (c *Channel) Close() error {
	if c.subscription != nil {
		return c.subscription.Close()
	}
	return nil
}

//--------------------------------------------------------------------------------------
// CLIENT
//--------------------------------------------------------------------------------------

type liveClientHandler struct {
	client *Client
}

func (h *liveClientHandler) OnConnect(_ *centrifuge.Client, e centrifuge.ConnectEvent) {
	h.client.log.Info("Connected to Grafana live", "clientId", e.ClientID)
	h.client.connected = true
}

func (h *liveClientHandler) OnError(_ *centrifuge.Client, e centrifuge.ErrorEvent) {
	h.client.log.Warn("Grafana live error", "error", e.Message)
}

func (h *liveClientHandler) OnDisconnect(_ *centrifuge.Client, e centrifuge.DisconnectEvent) {
	h.client.log.Info("Disconnected from Grafana live", "reason", e.Reason)
	h.client.connected = false
}

//--------------------------------------------------------------------------------------
// Channel
//--------------------------------------------------------------------------------------

type liveChannelHandler struct {
	channel    *Channel
	closeOnce  sync.Once
	msgHandler MessageHandler
	closeCh    chan struct{}
}

func (s *liveChannelHandler) handlePublication(pub *Message) {
	select {
	case <-s.closeCh:
	default:
		_ = s.msgHandler(pub)
	}
}

// OnSubscribeSuccess is called when the channel is subscribed.
func (s *liveChannelHandler) OnPublish(sub *centrifuge.Subscription, e centrifuge.PublishEvent) {
	s.channel.client.log.Debug("Publication", "channel", sub.Channel())
	s.handlePublication(&Message{Data: e.Data})
}

// OnSubscribeSuccess is called when the channel is subscribed.
func (s *liveChannelHandler) OnSubscribeSuccess(sub *centrifuge.Subscription, _ centrifuge.SubscribeSuccessEvent) {
	s.channel.subscribed = true
	s.channel.client.log.Info("Subscribed", "channel", sub.Channel())
}

// OnSubscribeError is called when the channel has an error.
func (s *liveChannelHandler) OnSubscribeError(sub *centrifuge.Subscription, e centrifuge.SubscribeErrorEvent) {
	s.channel.subscribed = false
	s.channel.client.log.Warn("Subscription failed", "channel", sub.Channel(), "error", e.Error)
}

// OnUnsubscribe is called when the channel is unsubscribed.
func (s *liveChannelHandler) OnUnsubscribe(sub *centrifuge.Subscription, _ centrifuge.UnsubscribeEvent) {
	s.channel.subscribed = false
	s.channel.client.log.Info("Unsubscribed", "channel", sub.Channel())
}

func (s *liveChannelHandler) Close() {
	s.closeOnce.Do(func() {
		close(s.closeCh)
	})
}

// NewClient initializes a client to communicate with Grafana Live server.
func NewClient(grafanaURL string) (*Client, error) {
	url, err := toWebSocketURL(grafanaURL)
	if err != nil {
		return nil, err
	}
	c := centrifuge.New(url, centrifuge.DefaultConfig())

	glc := &Client{
		client:   c,
		channels: make(map[string]*Channel),
		log:      backend.Logger,
	}
	handler := &liveClientHandler{
		client: glc,
	}
	c.OnConnect(handler)
	c.OnError(handler)
	c.OnDisconnect(handler)

	err = c.Connect()
	if err != nil {
		return nil, err
	}

	return glc, nil
}
