package pubsub

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gorilla/websocket"
)

const (
	PUBLISH     = "publish"
	SUBSCRIBE   = "subscribe"
	UNSUBSCRIBE = "unsubscribe"
)

type PubSub struct {
	Clients       []*Client
	Subscriptions []Subscription
	mu            sync.RWMutex
}

type Client struct {
	Id         string
	Connection *websocket.Conn
	WriteMu    sync.Mutex
}

type Message struct {
	Action  string          `json:"action"`
	Topic   string          `json:"topic"`
	Message json.RawMessage `json:"message,omitempty"`
}

type Subscription struct {
	Topic  string
	Client *Client
}

func (ps *PubSub) AddClient(client *Client) *PubSub {
	ps.mu.Lock()
	ps.Clients = append(ps.Clients, client)
	ps.mu.Unlock()

	fmt.Println("Adding new client:", client.Id)

	_ = client.SendJSON(map[string]interface{}{
		"type":      "connected",
		"client_id": client.Id,
	})

	return ps
}
func (ps *PubSub) RemoveClient(client *Client) *PubSub {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Remove client.
	clients := ps.Clients[:0]

	for _, c := range ps.Clients {
		if c.Id != client.Id {
			clients = append(clients, c)
		}
	}

	ps.Clients = clients

	// Remove subscriptions belonging to client.
	subscriptions := ps.Subscriptions[:0]

	for _, sub := range ps.Subscriptions {
		if sub.Client.Id != client.Id {
			subscriptions = append(subscriptions, sub)
		}
	}

	ps.Subscriptions = subscriptions

	fmt.Println("Removed client:", client.Id)

	return ps
}

func (ps *PubSub) GetSubscription(
	topic string,
	client *Client,
) []Subscription {

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var subscriptionList []Subscription

	for _, subscription := range ps.Subscriptions {
		if subscription.Topic != topic {
			continue
		}

		if client != nil && subscription.Client.Id != client.Id {
			continue
		}

		subscriptionList = append(
			subscriptionList,
			subscription,
		)
	}

	return subscriptionList
}
func (ps *PubSub) Subscribe(
	client *Client,
	topic string,
) *PubSub {

	// Prevent duplicate subscription.
	existing := ps.GetSubscription(topic, client)

	if len(existing) > 0 {
		fmt.Println(
			"client already subscribed:",
			client.Id,
			topic,
		)

		return ps
	}

	ps.mu.Lock()

	ps.Subscriptions = append(
		ps.Subscriptions,
		Subscription{
			Topic:  topic,
			Client: client,
		},
	)

	ps.mu.Unlock()

	fmt.Println(
		"new subscriber:",
		client.Id,
		"topic:",
		topic,
	)

	return ps
}

func (ps *PubSub) Unsubscribe(
	client *Client,
	topic string,
) *PubSub {

	ps.mu.Lock()
	defer ps.mu.Unlock()

	subscriptions := ps.Subscriptions[:0]

	for _, sub := range ps.Subscriptions {
		if sub.Client.Id == client.Id &&
			sub.Topic == topic {
			continue
		}

		subscriptions = append(
			subscriptions,
			sub,
		)
	}

	ps.Subscriptions = subscriptions

	fmt.Println(
		"client unsubscribed:",
		client.Id,
		topic,
	)

	return ps
}
func (ps *PubSub) Publish(
	sender *Client,
	topic string,
	message json.RawMessage,
) *PubSub {

	subscriptions := ps.GetSubscription(topic, nil)

	payload := Message{
		Action:  PUBLISH,
		Topic:   topic,
		Message: message,
	}

	for _, subscription := range subscriptions {
		client := subscription.Client

		if client == nil || client.Connection == nil {
			continue
		}

		if err := client.SendJSON(payload); err != nil {
			fmt.Println(
				"failed sending to client:",
				client.Id,
				err,
			)
		}
	}

	return ps
}
func (client *Client) SendJSON(data interface{}) error {
	client.WriteMu.Lock()
	defer client.WriteMu.Unlock()

	return client.Connection.WriteJSON(data)
}

func (ps *PubSub) HandleReceiveMessage(
	client *Client,
	messageType int,
	payload []byte,
) *PubSub {

	var message Message

	err := json.Unmarshal(payload, &message)

	if err != nil {
		fmt.Println(
			"payload is not correct:",
			err,
		)

		_ = client.SendJSON(map[string]interface{}{
			"type":    "error",
			"message": "invalid payload",
		})

		return ps
	}

	if message.Action == "" {
		_ = client.SendJSON(map[string]interface{}{
			"type":    "error",
			"message": "action is required",
		})

		return ps
	}

	if message.Topic == "" {
		_ = client.SendJSON(map[string]interface{}{
			"type":    "error",
			"message": "topic is required",
		})

		return ps
	}

	fmt.Println(
		"payload:",
		message.Action,
		message.Topic,
		string(message.Message),
	)

	switch message.Action {

	case PUBLISH:

		ps.Publish(
			client,
			message.Topic,
			message.Message,
		)

	case SUBSCRIBE:

		ps.Subscribe(
			client,
			message.Topic,
		)

		_ = client.SendJSON(map[string]interface{}{
			"type":    "subscribed",
			"topic":   message.Topic,
			"message": "subscription successful",
		})

	case UNSUBSCRIBE:

		ps.Unsubscribe(
			client,
			message.Topic,
		)

		_ = client.SendJSON(map[string]interface{}{
			"type":  "unsubscribed",
			"topic": message.Topic,
		})

	default:

		_ = client.SendJSON(map[string]interface{}{
			"type":    "error",
			"message": "unknown action",
		})
	}

	return ps
}
