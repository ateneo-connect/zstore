package http

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Define a custom type for the context key to avoid collisions
type contextKey string

const (
	subjectContextKey contextKey = "sub"
)

func JwtAuth(original func(w http.ResponseWriter, r *http.Request), publicKey *ecdsa.PublicKey) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header["Authorization"]
		if authHeader == nil {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}

		authHeaderParts := strings.Split(authHeader[0], " ")

		if len(authHeaderParts) != 2 || strings.ToLower(authHeaderParts[0]) != "bearer" {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}

		token, err := jwt.Parse(authHeaderParts[1], func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
				return nil, errors.New("unexpected signing method")
			}
			return publicKey, nil
		})

		if err != nil || !token.Valid {
			http.Error(w, "not authorized", http.StatusUnauthorized)
			return
		}

		if claims, ok := token.Claims.(jwt.MapClaims); ok {
			if sub, ok := claims["sub"].(string); ok {
				ctx := r.Context()
				ctx = context.WithValue(ctx, subjectContextKey, sub)
				r = r.WithContext(ctx)
			}
		}

		original(w, r)
	}
}

// func validateToken(accessToken string, publicKey *ecdsa.PublicKey) bool {
// 	token, err := jwt.Parse(accessToken, func(token *jwt.Token) (interface{}, error) {
// 		// Ensure the signing method is ES384 (ECDSA with SHA-384)
// 		if _, ok := token.Method.(*jwt.SigningMethodECDSA); !ok {
// 			return nil, errors.New("unexpected signing method")
// 		}
// 		return publicKey, nil
// 	})

// 	if err != nil {
// 		return false
// 	}

// 	return token.Valid
// }

func generateJwtToken(username string, privateKey *ecdsa.PrivateKey) (string, error) {
	// Create a new token object, specifying signing method and the claims
	token := jwt.NewWithClaims(jwt.SigningMethodES384, jwt.MapClaims{
		"sub": username,
		"exp": time.Now().Add(time.Hour * 24).Unix(),
		"iat": time.Now().Unix(),
	})

	// Sign and get the complete encoded token as a string using the private key
	tokenString, err := token.SignedString(privateKey)

	if err != nil {
		return "", fmt.Errorf("failed to generate JWT token: %w", err)
	}

	return tokenString, nil
}
