# gmailfs

A fast, read-only FUSE filesystem that mounts your Gmail mailbox. Every label
becomes a folder, every email a `.eml` file, organized by year, month, and day.
Aggressive caching and incremental sync.

```
/mnt/gmail/
  All Mail/2024/01/15/2024-01-15T143022-Subject.eml
  Inbox/2026/02/27/...
  Sent/...
```

By making email look like files, any tool that reads files works for free —
browse with `ls`, search with `grep`, read with `cat`. This also makes it
straightforward to use with AI coding assistants like Claude Code, which already
know how to navigate filesystems.


## Key features

- **Aggressive caching.** Listings and message metadata are cached in PebbleDB.
  Background history sync invalidates only what changed — usually just a single
  day.
- **Only populated directories are shown.** Years, months, and days are
  discovered by scanning backwards from the newest message. Empty periods cost
  zero API calls.
- **Synthetic "All Mail" label.** Gmail's API doesn't expose one, so gmailfs
  synthesizes it by querying without a label filter.
- **No dependencies.** Single static binary. Just needs `fusermount3`.
- **Read-only.** The filesystem is mounted read-only. Gmail data is never
  modified.


## How it compares

**Gmail MCP servers** (e.g.
[Gmail-MCP-Server](https://github.com/GongRzhe/Gmail-MCP-Server)) expose Gmail
through tool calls. This works, but ties you to a specific AI assistant and
protocol. gmailfs exposes email as plain files, so any tool — `grep`, `cat`, an
AI assistant, a script — can use it without integration work.

**The original [GmailFS](https://en.wikipedia.org/wiki/GmailFS) (2004)** and
similar projects like
[GmailStorejFS](https://github.com/subhamX/GmailStorejFS) use Gmail as generic
*storage*, encoding arbitrary files into email attachments. gmailfs does the
opposite: it makes your actual email readable as files.

**[lucastliu/gmailfs](https://github.com/lucastliu/gmailfs)** is closer in
spirit — it mounts Gmail for reading and sending. gmailfs focuses on the
read-only case with a chronological hierarchy, aggressive caching, and
incremental history sync to minimize API usage.


## Getting started


### Prerequisites

- Go 1.25+
- FUSE support (`fusermount3` — install `fuse3` on Debian/Ubuntu, `fuse3` on
  Fedora)
- A Google Cloud project with the Gmail API enabled and an OAuth2 desktop
  credentials file


### Setup

1. Set up a Google Cloud project:

   - [Enable the Gmail API](https://console.cloud.google.com/apis/library/gmail.googleapis.com).
   - Create OAuth2 desktop credentials in the
     [Credentials console](https://console.cloud.google.com/apis/credentials)
     and save the JSON as `~/.config/gmailfs/credentials.json`.

2. Build and run:

   ```sh
   GOBIN="$PWD/bin" go install .
   ./bin/gmailfs -mountpoint /mnt/gmail
   ```

   On first run, gmailfs will print a URL to authorize with your Google account.
   Open it in a browser, authorize, and paste the code back.

3. Browse your email:

   ```sh
   ls /mnt/gmail/
   ls "/mnt/gmail/All Mail/2026/"
   cat "/mnt/gmail/Inbox/2026/02/27/2026-02-27T091500-Hello.eml"
   ```


### Options

```
-mountpoint    FUSE mount point (required)
-config-dir    Config directory for credentials/token (default: ~/.config/gmailfs)
-cache-dir     PebbleDB cache directory (default: ~/.cache/gmailfs)
-sync-interval History sync polling interval (default: 30s)
-debug         Enable debug logging
-fuse-debug    Enable FUSE debug logging
```
