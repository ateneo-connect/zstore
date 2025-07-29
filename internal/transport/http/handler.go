package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	logger "github.com/chi-middleware/logrus-logger"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/config"
)

type Handler interface {
	mapRoutes(router chi.Router)
}

type MainHandler struct {
	Router   chi.Router
	Handlers []Handler
	Server   *http.Server
}

func NewMainHandler(cfg *config.Config) *MainHandler {
	h := &MainHandler{
		Handlers: []Handler{},
	}

	h.Router = chi.NewRouter()

	h.Router.Use(cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			http.MethodHead,
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
		},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: false,
	}))

	h.Router.Use(logger.Logger("router", log.New()))
	h.Router.Use(JSONMiddleware)
	h.Router.Use(TimeoutMiddleware)

	// h.mapRoutes()

	h.Server = &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", cfg.Port),
		Handler: h.Router,
	}

	return h
}

func (h *MainHandler) AddHandler(handler Handler) {
	h.Handlers = append(h.Handlers, handler)
}

func (h *MainHandler) MapRoutes() {
	h.Router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Hello world",
		})
	})

	for _, handler := range h.Handlers {
		handler.mapRoutes(h.Router)
		chi.Walk(h.Router, func(method string, route string, handler http.Handler, middlewares ...func(http.Handler) http.Handler) error {
			log.Debugf("[%s]: '%s' has %d middlewares\n", method, route, len(middlewares))
			return nil
		})
	}
}

func (h *MainHandler) Serve() error {

	go func() {
		if err := h.Server.ListenAndServe(); err != nil {
			log.Error(err)
		}
	}()

	c := make(chan os.Signal, 1)

	signal.Notify(c, os.Interrupt)
	<-c

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

	defer cancel()

	h.Server.Shutdown(ctx)

	log.Warn("shutting down gracefully")

	return nil
}
