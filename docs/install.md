# Installation

## Via cargo-binstall (all platforms)

If you have [`cargo-binstall`](https://github.com/cargo-bins/cargo-binstall) installed, you can skip compilation entirely:

```sh
cargo binstall ringdrop
```

> **Note:** pre-built binary downloads are supported from **v0.11.0** onwards.
> Installing an older version falls back to compiling from source automatically.

---

## Fedora 42+

### Via DNF COPR (recommended)

```sh
dnf copr enable rikettsie/ringdrop
dnf install ringdrop
```

To upgrade later:

```sh
dnf upgrade ringdrop
```

---

## Linux (all distributions)

### Via Cargo

Requires Rust. If you don't have it yet:

```sh
curl https://sh.rustup.rs -sSf | sh
```

Then install `rdrop`:

```sh
cargo install ringdrop
```

After installation, make sure `~/.cargo/bin` is in your `PATH`:

```sh
export PATH="$HOME/.cargo/bin:$PATH"
```

Add that line to `~/.bashrc`, `~/.zshrc`, or equivalent to make it permanent.

---

## macOS

### Via install script (recommended)

Handles both first-time install and upgrades:

```sh
curl -fsSL https://raw.githubusercontent.com/rikettsie/ringdrop/main/install/install.sh | sh
```

Requires [Homebrew](https://brew.sh). If you don't have it:

```sh
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Manually via Homebrew

```sh
brew tap rikettsie/tap
brew install rdrop        # first install
brew upgrade rdrop        # upgrade
```

---

## Windows

### Via install script (recommended)

Run in PowerShell. Handles both first-time install and upgrades:

```powershell
irm https://raw.githubusercontent.com/rikettsie/ringdrop/main/install/install.ps1 | iex
```

Requires [Scoop](https://scoop.sh). If you don't have it:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
irm get.scoop.sh | iex
```

### Manually via Scoop

```powershell
scoop bucket add rikettsie https://github.com/rikettsie/scoop-bucket
scoop install rdrop       # first install
scoop update rdrop        # upgrade
```

---

## Verify the installation

```sh
rdrop --version
```

---

## Desktop GUI (optional)

[ringdrop-gui](https://github.com/rikettsie/ringdrop-gui) is a Tauri v2 desktop app available for Linux, macOS, and Windows. It connects to the local ringdrop daemon over IPC and exposes the full `rdrop` feature set as a native UI.

Pre-built installers (`.AppImage`, `.deb`, `.rpm`, `.dmg`, `.msi`) are available on the [releases page](https://github.com/rikettsie/ringdrop-gui/releases/latest).

**Prerequisite:** the ringdrop daemon must be installed and running (`rdrop daemon start`) before launching the GUI.
