// Package http provides HTTP handlers for user profile management
package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-chi/chi/v5"
	log "github.com/sirupsen/logrus"
	"github.com/zzenonn/zstore/internal/config"
)

// UserProfileService defines the interface for profile management operations
type UserProfileService interface {
	// UploadProfile uploads a user's profile image
	UploadProfile(ctx context.Context, username string, key string, r io.Reader) error
	// GeneratePresignedURL generates a pre-signed URL for accessing a profile image
	GeneratePresignedURL(ctx context.Context, username string, key string) (string, error)
	// DeleteProfile removes a user's profile image
	DeleteProfile(ctx context.Context, username string, key string) error
}

// UserProfileHandler handles HTTP requests for user profile operations
type UserProfileHandler struct {
	Service UserProfileService
	Config  *config.Config
}

// NewUserProfileHandler creates a new UserProfileHandler instance
func NewUserProfileHandler(s UserProfileService, cfg *config.Config) *UserProfileHandler {
	return &UserProfileHandler{
		Service: s,
		Config:  cfg,
	}
}

// PutProfile handles PUT requests to upload a user's profile image
func (h *UserProfileHandler) PutProfile(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received PUT /api/v1/users/{username}/profile request")

	// Extract and validate username from URL parameters
	username := chi.URLParam(r, "username")
	if username == "" {
		log.Error("No username provided in request")
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}

	// Get sub claim from JWT token context
	subVal := r.Context().Value(subjectContextKey)

	sub, ok := subVal.(string)
	if !ok || sub != username {
		log.Error("Token sub does not match username or sub is missing. Sub value: ", sub)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse multipart form with 10MB size limit
	err := r.ParseMultipartForm(10 << 20) // 10MB max
	if err != nil {
		log.Error("Failed to parse multipart form: ", err)
		http.Error(w, "Invalid multipart form", http.StatusBadRequest)
		return
	}

	// Retrieve and validate file from form
	file, _, err := r.FormFile("file")
	if err != nil {
		log.Error("Failed to read file: ", err)
		http.Error(w, "File upload is required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Upload profile image using the service
	err = h.Service.UploadProfile(r.Context(), username, "profile.jpg", file)
	if err != nil {
		log.Error("Error uploading profile: ", err)
		http.Error(w, "Failed to upload profile", http.StatusInternalServerError)
		return
	}

	// Return success response
	log.Debug(fmt.Sprintf("Profile uploaded successfully for user: %s", username))
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(Response{Message: "Profile uploaded successfully"})
}

// GetProfile handles GET requests to retrieve a user's profile image URL
func (h *UserProfileHandler) GetProfile(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received GET /api/v1/users/{username}/profile request")

	// Extract and validate username from URL parameters
	username := chi.URLParam(r, "username")
	if username == "" {
		log.Error("No username provided in request")
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}

	// Generate pre-signed URL for profile image
	file, err := h.Service.GeneratePresignedURL(r.Context(), username, "profile.jpg")
	if err != nil {
		log.Error("Error downloading profile: ", err)
		http.Error(w, "Failed to download profile", http.StatusInternalServerError)
		return
	}

	// Prepare and send JSON response
	w.Header().Set("Content-Type", "application/json")

	response := struct {
		ProfileURL string `json:"profile_url"`
	}{
		ProfileURL: file,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Error("Error encoding response: ", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}
}

// DeleteProfile handles DELETE requests to remove a user's profile image
func (h *UserProfileHandler) DeleteProfile(w http.ResponseWriter, r *http.Request) {
	log.Debug("Received DELETE /api/v1/users/{username}/profile request")

	// Extract and validate username from URL parameters
	username := chi.URLParam(r, "username")
	if username == "" {
		log.Error("No username provided in request")
		http.Error(w, "Username is required", http.StatusBadRequest)
		return
	}

	// Delete profile image using the service
	err := h.Service.DeleteProfile(r.Context(), username, "profile.jpg")
	if err != nil {
		log.Error("Error deleting profile: ", err)
		http.Error(w, "Failed to delete profile", http.StatusInternalServerError)
		return
	}

	// Return success response
	log.Debug(fmt.Sprintf("Profile deleted successfully for user: %s", username))
	json.NewEncoder(w).Encode(Response{Message: "Profile deleted successfully"})
}

// mapRoutes sets up the routing for the profile management endpoints
func (h *UserProfileHandler) mapRoutes(router chi.Router) {
	router.Route("/api/v1/users/{username}/profile", func(r chi.Router) {
		r.Put("/", JwtAuth(h.PutProfile, h.Config.ECDSAPublicKey))       // Protected endpoint
		r.Get("/", h.GetProfile)                                         // Public endpoint
		r.Delete("/", JwtAuth(h.DeleteProfile, h.Config.ECDSAPublicKey)) // Protected endpoint
	})
}
