package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-playground/validator/v10"
	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/config"
	"github.com/zzenonn/zstore/internal/domain"
)

type UserService interface {
	GetUser(ctx context.Context, id string) (domain.User, error)
	UpdateUser(ctx context.Context, u domain.User) (domain.User, error)
	DeleteUser(ctx context.Context, id string) error
	CreateUser(ctx context.Context, u domain.User) (domain.User, error)
	Login(ctx context.Context, username string, password string) error
}

type Token struct {
	Token string `json:"token,omitempty"`
}

type UserHandler struct {
	Service    UserService
	JwtSignKey []byte
	Config     *config.Config
}

type PostUserRequest struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password" validate:"required"`
}

func convertPostUserRequestToUser(u PostUserRequest) domain.User {
	return domain.User{
		Username: &u.Username,
		Password: u.Password,
	}
}

func NewUserHandler(s UserService, cfg *config.Config) *UserHandler {
	h := &UserHandler{
		Service: s,
		Config:  cfg,
	}

	return h
}

func (h *UserHandler) PostUser(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received POST /api/v1/users request")

	var u PostUserRequest
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		log.Error("Error decoding request body: ", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	validate := validator.New()
	err := validate.Struct(u)
	if err != nil {
		log.Debug("Validation failed for user: ", u)
		http.Error(w, "Not a valid user", http.StatusBadRequest)
		return
	}

	convertedUser := convertPostUserRequestToUser(u)
	log.Debug(fmt.Sprintf("Converted user data: %#v", convertedUser))

	createdUser, err := h.Service.CreateUser(r.Context(), convertedUser)
	if err != nil {
		log.Error("Error creating user: ", err)
		http.Error(w, "Failed to create user", http.StatusInternalServerError)
		return
	}

	log.Debug(fmt.Sprintf("User created successfully: %#v", createdUser))

	if err := json.NewEncoder(w).Encode(createdUser); err != nil {
		log.Fatal("Error encoding response: ", err)
	}
}

func (h *UserHandler) GetUser(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received GET /api/v1/users/{username} request")

	username := chi.URLParam(r, "username")
	if username == "" {
		log.Error("No username provided in request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Debug(fmt.Sprintf("Fetching user with ID: %s", username))
	u, err := h.Service.GetUser(r.Context(), username)
	if err != nil {
		log.Error("Error fetching user: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Debug(fmt.Sprintf("User fetched successfully: %#v", u))

	if err := json.NewEncoder(w).Encode(u); err != nil {
		log.Fatal("Error encoding response: ", err)
	}
}

func (h *UserHandler) UpdateUser(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received PUT /api/v1/users/{username} request")

	username := chi.URLParam(r, "username")
	if username == "" {
		log.Debug("No username provided in request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	var u domain.User
	if err := json.NewDecoder(r.Body).Decode(&u); err != nil {
		log.Error("Error decoding request body: ", err)
		return
	}

	log.Debug(fmt.Sprintf("Updating user with ID: %s", username))

	u, err := h.Service.UpdateUser(r.Context(), u)
	if err != nil {
		log.Error("Error updating user: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Debug(fmt.Sprintf("User updated successfully: %#v", u))

	if err := json.NewEncoder(w).Encode(u); err != nil {
		log.Fatal("Error encoding response: ", err)
	}
}

func (h *UserHandler) DeleteUser(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received DELETE /api/v1/users/{username} request")

	username := chi.URLParam(r, "username")
	if username == "" {
		log.Debug("No username provided in request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	log.Debug(fmt.Sprintf("Deleting user with ID: %s", username))

	err := h.Service.DeleteUser(r.Context(), username)
	if err != nil {
		log.Error("Error deleting user: ", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.Debug("User deleted successfully")

	if err := json.NewEncoder(w).Encode(Response{Message: "Successfully deleted"}); err != nil {
		log.Fatal("Error encoding response: ", err)
	}
}

func (h *UserHandler) Login(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received POST /api/v1/users/login request")

	var m map[string]string
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		log.Error("Error decoding request body: ", err)
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	username := m["username"]
	password := m["password"]

	log.Debug(fmt.Sprintf("Attempting login for user: %s", username))

	err := h.Service.Login(r.Context(), username, password)
	if err != nil {
		log.Error("Login failed: ", err)
		http.Error(w, "Not authorized", http.StatusUnauthorized)
		return
	}

	tokenString, err := generateJwtToken(username, h.Config.ECDSAPrivateKey)
	if err != nil {
		log.Error("Error generating JWT token: ", err)
		http.Error(w, "Failed to generate token", http.StatusInternalServerError)
		return
	}

	log.Debug("JWT token generated successfully")

	token := Token{
		Token: tokenString,
	}

	if err := json.NewEncoder(w).Encode(token); err != nil {
		log.Error("Error encoding response: ", err)
	}
}

func (h *UserHandler) mapRoutes(router chi.Router) {
	router.Route("/api/v1/users", func(r chi.Router) {
		r.Post("/", h.PostUser) //JwtAuth(h.PostUser, h.Config.ECDSAPublicKey))
		r.Post("/login", h.Login)

		// r.Route("/{username}", func(r chi.Router) {
		// 	r.Get("/", h.GetUser)
		// 	r.Put("/", h.UpdateUser)
		// 	r.Delete("/", h.DeleteUser)
		// })
	})
}
