package domain

import "golang.org/x/crypto/bcrypt"

// User - representation of a user in the system
type User struct {
	Username       *string `json:"username,omitempty" dynamodbav:"pk,omitempty"`
	Password       string  `json:"-" dynamodbav:"-"`
	HashedPassword []byte  `json:"-" dynamodbav:"hashed_password,omitempty"`
}

// HashPassword - hashes the user's password
func (u *User) HashPassword() error {
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(u.Password), 10)
	if err != nil {
		return err
	}
	u.HashedPassword = hashedPassword
	u.Password = "" // Clear plain text password after hashing
	return nil
}
