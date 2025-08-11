package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Load environment variables
func init() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Printf("Warning: Could not load .env file: %v", err)
	}
}

// KafkaProducer wraps sarama.AsyncProducer with lifecycle management
type KafkaProducer struct {
	producer sarama.AsyncProducer
}

// NewKafkaProducer creates a new Kafka producer
func NewKafkaProducer() (*KafkaProducer, error) {
	brokers := strings.Split(os.Getenv("KAFKA_BOOTSTRAP_SERVERS"), ",")
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return &KafkaProducer{producer: producer}, nil
}

// Close stops the producer
func (kp *KafkaProducer) Close() error {
	return kp.producer.Close()
}

// TrackEventType represents possible track event types
type TrackEventType string

const (
	Play               TrackEventType = "play"
	Pause              TrackEventType = "pause"
	Skip               TrackEventType = "skip"
	Like               TrackEventType = "like"
	Dislike            TrackEventType = "dislike"
	AddToPlaylist      TrackEventType = "add_to_playlist"
	RemoveFromPlaylist TrackEventType = "remove_from_playlist"
)

// BaseAction contains common fields for all actions
type BaseAction struct {
	ActionID   uuid.UUID `json:"action_id"`
	ActionTime time.Time `json:"action_time"`
	UserID     uuid.UUID `json:"user_id"`
}

// TrackEvent represents a track-related event
type TrackEvent struct {
	BaseAction
	ActionType    TrackEventType `json:"action_type"`
	TrackID       uuid.UUID      `json:"track_id"`
	Recommended   bool           `json:"recommended"`
	PlaylistID    *uuid.UUID     `json:"playlist_id,omitempty"`
	Duration      *int           `json:"duration,omitempty"`
}

// Custom JSON marshaling for TrackEvent to handle NULL values
func (te TrackEvent) MarshalJSON() ([]byte, error) {
	type Alias TrackEvent
	aux := &struct {
		*Alias
		PlaylistID interface{} `json:"playlist_id,omitempty"`
		Duration   interface{} `json:"duration,omitempty"`
	}{
		Alias: (*Alias)(&te),
	}

	if te.PlaylistID == nil {
		aux.PlaylistID = "NULL"
	} else {
		aux.PlaylistID = te.PlaylistID
	}

	if te.Duration == nil {
		aux.Duration = "NULL"
	} else {
		aux.Duration = te.Duration
	}

	return json.Marshal(aux)
}

// App holds application dependencies
type App struct {
	KafkaProducer *KafkaProducer
	Echo          *echo.Echo
}

// NewApp creates a new application instance
func NewApp() (*App, error) {
	e := echo.New()
	e.Use(middleware.Recover())
	e.Use(middleware.Logger())

	producer, err := NewKafkaProducer()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
	}

	return &App{
		Echo:          e,
		KafkaProducer: producer,
	}, nil
}

// Close cleans up application resources
func (a *App) Close() error {
	if err := a.KafkaProducer.Close(); err != nil {
		return fmt.Errorf("error closing Kafka producer: %w", err)
	}
	return nil
}

// createTrackEvent handles the track event creation
func (a *App) createTrackEvent(c echo.Context) error {
	var event TrackEvent
	if err := c.Bind(&event); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	eventJSON, err := json.Marshal(event)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	msg := &sarama.ProducerMessage{
		Topic: "track",
		Key:   sarama.StringEncoder(event.UserID.String()),
		Value: sarama.StringEncoder(eventJSON),
	}

	a.KafkaProducer.producer.Input() <- msg

	select {
	case <-a.KafkaProducer.producer.Successes():
		return c.JSON(http.StatusOK, map[string]interface{}{
			"status":  "success",
			"message": "Data sent to Kafka",
		})
	case err := <-a.KafkaProducer.producer.Errors():
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
}

func main() {
	app, err := NewApp()
	if err != nil {
		log.Fatalf("Failed to initialize application: %v", err)
	}
	defer app.Close()

	// Setup routes
	app.Echo.POST("/tracks", app.createTrackEvent)

	// Start server
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	if err := app.Echo.Start(":" + port); err != nil {
		log.Printf("Server error: %v", err)
	}
}