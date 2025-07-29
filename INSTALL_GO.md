# Installing Go 1.24.3 on Amazon Linux (amd64)

## Step 1: Download the Go Archive

Use `wget` to download the Go tarball:

```bash
wget https://go.dev/dl/go1.24.3.linux-amd64.tar.gz
````

---

## Step 2: Remove Any Existing Go Installation

If Go is already installed, remove it to prevent conflicts:

```bash
sudo rm -rf /usr/local/go
```

---

## Step 3: Extract the Archive to /usr/local

Extract the downloaded archive to `/usr/local`:

```bash
sudo tar -C /usr/local -xzf go1.24.3.linux-amd64.tar.gz
```

---

## Step 4: Set Up Environment Variables

Append the Go binary path and GOPATH to your shell profile using `echo`:

```bash
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bash_profile
echo 'export GOPATH=$HOME/go' >> ~/.bash_profile
echo 'export PATH=$PATH:$GOPATH/bin' >> ~/.bash_profile
```

Apply the changes:

```bash
source ~/.bash_profile
```

---

## Step 5: Verify the Installation

Confirm that Go is installed correctly:

```bash
go version
```

You should see output similar to:

```
go version go1.24.3 linux/amd64
```

---

## Optional: Set Up Go Workspace

To set up a Go workspace, define the `GOPATH`:

```bash
export GOPATH=$HOME/go
export PATH=$PATH:$GOPATH/bin
```

Add the lines to your shell profile and apply with:

```bash
source ~/.bash_profile
```

---

For more details, refer to the [official Go install guide](https://go.dev/doc/install).

