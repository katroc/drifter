# Drifter

**Drifter** is a secure, TUI-based S3 multipart uploader designed for high-assurance environments. It acts as a "tiered storage" airlock, allowing you to stage files, scan them for malware using ClamAV, and upload them to S3-compatible storage only after they are proven clean.

![Drifter TUI](assets/drifter.png)

## Key Features

- **🛡️ Secure Pipeline**: File -> Staging -> ClamAV Scan -> S3 Upload. Threats are automatically quarantined.
- **🚀 Multipart Uploads**: Efficiently handles large files (GBs/TBs) with concurrent chunk uploads.
- **👁️ Live TUI**: Real-time progress bars, status indicators, and file browsing in a terminal interface.
- **🧹 Auto-Cleanup**: Successfully uploaded files are automatically removed from local staging to save space.
- **🎨 Theming**: 10+ built-in themes (Nord, Tokyo Night, High Contrast, Transparent) with configurable border styles.
- **⚙️ Config Wizard**: Built-in first-run wizard to guide you through setup.

## Installation

### Prerequisites

- **Rust**: `cargo` (for building the app)
- **Docker**: (for running the ClamAV scanner)

### 1. Start ClamAV Scanner

Drifter requires a ClamAV instance reachable via TCP. We've provided a Docker Compose file for quick setup:

```bash
cd docker
docker compose up -d
```

This starts ClamAV on `localhost:3310`.

### 2. Installation Methods

#### Option A: Build and Run Locally (Development)
```bash
# Return to root directory
cd ..

# Build release binary
cargo build --release

# Run
./target/release/drifter
```

#### Option B: Install System-Wide (Recommended)
This installs the `drifter` binary to your Cargo bin path (typically `~/.cargo/bin`), allowing you to run it from anywhere.

```bash
# Install from source
cargo install --path .

# Run from anywhere
drifter
```

#### Option C: Copy Binary to Another Machine
The built binary is self-contained (mostly static, depends on standard glibc). You can scp it to other Linux machines:

```bash
# Build (if not already built)
cargo build --release

# Copy to remote server
scp target/release/drifter user@remote-server:/usr/local/bin/
```

## Setup & Configuration

On first run, Drifter will detect a missing configuration and launch the **Setup Wizard**.

You will be prompted to configure:
1.  **Directories**: Staging (temp storage) and Quarantine locations.
2.  **Scanner**: Host/Port for ClamAV (default: `127.0.0.1:3310`).
3.  **S3**: Bucket name, Region, Endpoint, and Credentials.
4.  **Performance**: Chunk sizes and concurrency levels.

_Settings are stored securely in a local SQLite database (`state/drifter.db`)._

## Usage Workflow

Drifter is designed for keyboard-driven efficiency.

### 1. The Rail (Left Sidebar)
Navigate between tabs using `Tab` or `Up/Down`.
- **Transfers**: Main view. Browser on left, Transfer Queue on right.
- **Quarantine**: View files flagged as threats.
- **Settings**: Configure S3, Scanner, and UI preferences.

### 2. File Browser (Hopper)
- `Up/Down`: Navigate files.
- `Right` / `Enter`: Enter directory.
- `Left`: Go up a directory.
- `Space`: **Select/Deselect** file for staging.
- `s`: **Stage** selected files (Adds to Queue).

### 3. Queue Management
- **Pending**: Files waiting to be processed.
- **Scanning**: Currently being checked by ClamAV.
- **Uploading**: Secure transfer to S3.
- **Done**: Upload complete (removed from list).
- **Quarantined**: Threat found (moved to Quarantine tab).

### Controls summary
- `Tab`: Switch focus between panels (Rail <-> Browser <-> Queue).
- `q`: Quit application.
- `?`: Toggle help (if implemented).

## Theming

Drifter looks great in any terminal. Go to **Settings > Theme** to change the look.
- **Presets**: Nord, Drifter, Tokyo Night, High Contrast, Transparent, etc.
- **Transparent Mode**: Support for terminals with background images/blur.
- **Borders**: Configurable styles (Rounded, Double, Plain).

## License

MIT
