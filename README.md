# Go API Template

This project serves as a template for building Go-based web applications, providing a structured foundation with key components such as configuration management, logging, database access, service layers, and HTTP transport. It follows a clean architecture approach, with a clear separation of concerns between different layers of the application.

## Installing Go
If you do not have go installed on your machine, you can follow these instructions [Installing Go](INSTALL_GO.md).

## Project Structure

The project is organized into several directories. Below is an overview of the project structure:

```
.
├── cmd
│   └── server
│       └── main.go                # Entry point for the application
├── internal                       # Core business logic and utilities
│   ├── config                     # Configuration management
│   ├── domain                     # Domain models
│   ├── errors                     # Custom error handling
│   ├── logging                    # Logging utilities
│   ├── repository                 # Data access layer
│   ├── service                    # Business logic and service layer
│   └── transport                  # Transport layer (HTTP)
├── migrations                     # Placeholder for database migration files
├── Dockerfile                     # Dockerfile for containerizing the Go application
├── docker-compose.yaml            # Docker Compose file for setting up services (e.g., database)
├── go.mod                         # Go module file, managing dependencies
├── go.sum                         # Go checksum file for dependencies
├── README.md                      # Project documentation (this file)
└── tests                          # Placeholder for test files
```

### Key Components

- **`cmd/server/main.go`**: The entry point for the application. It initializes configuration, logging, database connections, and starts the HTTP server.
  
- **`internal/config`**: Manages application configuration, loading values from environment variables.

- **`internal/domain`**: Contains domain models, such as `User`, which represents a user in the system.

- **`internal/errors`**: Defines custom error types used across the application.

- **`internal/logging`**: Configures logging using Logrus, allowing log levels and formats to be customized.

- **`internal/repository`**: Handles database interactions. This can be configured to work with various databases like Firestore, PostgreSQL, etc.

- **`internal/service`**: Contains business logic, such as user management (e.g., creating, updating, deleting users).

- **`internal/transport/http`**: Defines HTTP handlers, middleware, and JWT authentication logic.

## How to Use This Template

1. **Clone the repository**: Start by cloning this template repository to your local machine.
   ```bash
   git clone git@github.com:<your-username>/<your-repo-name>.git
   cd <your-repo-name>
   ```

2. **Modify the remote repository**: If you want to push this repository to a different remote (e.g., GitLab), you can rename the existing remote and add a new one.
   ```bash
   git remote rename origin old-origin
   git remote add origin git@gitlab.com:<your-group>/<your-new-repo>.git
   git push --set-upstream origin --all
   git push --set-upstream origin --tags
   ```

3. **Update import paths**: If you are moving the project from one repository to another, update the Go import paths accordingly.
   ```bash
   find . -name "*.go" -type f -exec sed -i -e 's|github.com/<old-username>/<old-repo>|gitlab.com/<new-group>/<new-repo>|g' {} \;
   ```

4. **Reinitialize Go modules**: Remove the old `go.mod` and `go.sum` files, and initialize a new Go module with the correct path.
   ```bash
   rm go.mod go.sum
   go mod init gitlab.com/<new-group>/<new-repo>
   go mod tidy
   ```

5. **Set up environment variables**: Configure the necessary environment variables for your application, such as `PROJECT_ID`, `PORT`, `LOG_LEVEL`, `ECDSA_PRIVATE_KEY_SECRET_PATH`, and `ECDSA_PUBLIC_KEY_SECRET_PATH` (used for JWT token signing). This is defined in `Taskfile.yaml`.

| Variable | Description |
|----------|-------------|
| **PROJECT_ID** | Identifies your project, likely used for AWS resource naming or organization. |
| **PORT** | Specifies which network port your Go API server will listen on (defaults to 8080 if not set). |
| **LOG_LEVEL** | Controls the verbosity of logging in your application. Common values include "debug", "info", "warn", and "error". |
| **ECDSA_PRIVATE_KEY_SECRET_PATH** | Path in AWS Parameter Store where your ECDSA private key is stored. Used for signing JWT tokens. Default: "/ecdsa/private-key". **Must be configured in AWS Parameter Store before running the application**. |
| **ECDSA_PUBLIC_KEY_SECRET_PATH** | Path in AWS Parameter where your ECDSA public key is stored. Used for verifying JWT tokens. Default: "/ecdsa/public-key". **Must be configured in AWS Parameter Store before running the application**. |


6. **Run the application**: You can run the application using `task run` or `go-task run` depending on how your system names the go-task utility.

7. **Extend the application**: Add new features, services, and routes as needed. The template provides a solid foundation for building scalable Go applications.

## Sample Usage with `curl`

Once the server is running, you can interact with the API using `curl`. Below are some sample requests:

### Create a User
```bash
curl -X POST http://localhost:8080/api/v1/users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer eyJhbGciOiJFUzM4NCIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3MzQ3ODI3NDksImlhdCI6MTczNDY5NjM0OSwic3ViIjoiY3NjaTYxLXVzZXIifQ.odEl6mgNTTrYlbOd57b85DUbEzX7DrLKBcsA__HAAk3CFweRV2_quM7H8EAr__toqwMyYebIWMSoGAIZ3UHwVl5VsGDnn2HXx90I0aUEt_BmoR3L1g81tJzhBU_AaMIC" \ 
  -d '{
    "username": "new-user",
    "password": "password123"
  }'
```

### Get a User
```bash
curl -X GET http://localhost:8080/api/v1/users/testuser
```

### Update a User
```bash
curl -X PUT http://localhost:8080/api/v1/users/testuser \
  -H "Content-Type: application/json" \
  -d '{
    "username": "updateduser",
    "password": "newpassword123"
  }'
```

### Delete a User
```bash
curl -X DELETE http://localhost:8080/api/v1/users/testuser
```

### Login
```bash
curl -X POST http://localhost:8080/api/v1/users/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "new-user",
    "password": "password123"
  }'
```

### Upload Profile
```bash
curl -X PUT http://localhost:8080/api/v1/users/new-user/profile \
  -H "Authorization: Bearer YOUR_JWT_TOKEN_HERE" \
  -F "file=@/path/to/profile.jpg"
```

## Conclusion

This template provides a well-structured starting point for Go projects, following best practices such as clean architecture and separation of concerns. It includes placeholders for configuration, logging, error handling, database access, and service layers, making it easy to extend and customize for specific use cases. The `http` package includes JWT authentication, middleware, and user-related handlers, making it easy to implement secure and scalable HTTP APIs.

This repository is meant to be **forked** or **cloned** and modified to fit your specific project needs.

