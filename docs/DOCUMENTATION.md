# Documentation site

The user documentation lives in [`documentation/`](../documentation). It is written in Markdown, follows the [Diátaxis](https://diataxis.fr/) framework, and is built with [Sphinx](https://www.sphinx-doc.org/), [MyST](https://myst-parser.readthedocs.io/) and Canonical's [Ulwazi](https://github.com/canonical/ulwazi) theme.

## Build and preview

You need `make`, `python3` and `python3-venv`.

```sh
make -C documentation run
```

builds the site, serves it at `http://127.0.0.1:8000` and rebuilds on every change. The first run creates `documentation/.venv` and installs `documentation/requirements.txt`.

| Target | Does |
|---|---|
| `make html` | Builds the site into `_build/html`. Warnings are errors |
| `make run` | Builds, serves and rebuilds on change |
| `make spelling` | Checks spelling with codespell. Add accepted words to `.wordlist.txt` |
| `make linkcheck` | Checks external links |
| `make check` | `html` and `spelling` |
| `make clean` | Removes `_build` |

## Add a page

1. Create the Markdown file in the right section: `tutorials/`, `how-to/`, `reference/` or `explanation/`.
2. Add it to the section's `index.md`: a row in the table, and an entry in the `{toctree}` block at the end. A page missing from a toctree fails the build.

Diagrams are written as ` ```mermaid ` blocks.

## Code examples are compiled

Every line of a ` ```rust ` block must also appear, ignoring indentation, in [`examples/guestbook`](../examples/guestbook) or [`examples/doc-snippets`](../examples/doc-snippets). Both crates are workspace members, so CI builds and tests them. Check with:

```sh
cargo xtask docs-check
```

When you add or change a code block, add or change the same lines in `examples/doc-snippets`, or in `examples/guestbook` for the tutorials. A block that is deliberately incomplete, such as a signature summary or a sketch with placeholders, uses ` ```{code-block} rust ` and is not checked.

## Continuous integration

[`.github/workflows/docs.yml`](../.github/workflows/docs.yml):

| Event | Jobs |
|---|---|
| Pull request touching the docs or examples | `docs-check`, `make html spelling` |
| Push to `main` | Checks, then deploys `latest` |
| Push of a tag `vX.Y.Z` | Checks, then deploys `X.Y.Z` and `stable` |
| Run by hand | Checks, `make linkcheck`, then deploys like a push |
| Weekly | Checks and `make linkcheck` |

Deploying is skipped until the repository variable `DOCS_HOST` is set.

## Self-hosting

The site is static files. Each version is a directory under one web root:

```text
/srv/quorium-docs/
├── latest/          main
├── stable/          the newest tag
└── 0.1.0-alpha.1/   each tag
```

The web server redirects `/` to `/latest/` and serves `/latest/404/` for missing pages. CI copies each build with `rsync` over SSH, as a user whose key can only run `rsync` inside the web root.

### Set up the server

On an Ubuntu 24.04 server reachable from GitHub Actions over SSH, with a DNS name pointing at it and ports 80 and 443 open:

1. Create a deploy key pair on your machine:

   ```sh
   ssh-keygen -t ed25519 -N "" -C quorium-docs-deploy -f quorium-docs-deploy
   ```

2. Copy `deploy/docs/` and `quorium-docs-deploy.pub` to the server and run:

   ```sh
   sudo deploy/docs/setup-server.sh --domain docs.example.com --deploy-key quorium-docs-deploy.pub
   ```

   This installs `rsync` and Caddy, creates the `quorium-docs` user and `/srv/quorium-docs`, restricts the key to `rrsync /srv/quorium-docs`, and configures Caddy, which obtains a TLS certificate from Let's Encrypt for the domain. Add `--ssh-host HOST` if CI reaches the server by a different name than the domain. The script prints the server's host key line.

   To use nginx instead, pass `--web-server none`, install [`deploy/docs/nginx.conf`](../deploy/docs/nginx.conf) with your domain in place of `docs.example.com`, and obtain a certificate with certbot.

3. In the GitHub repository settings, under Settings → Environments, create an environment named `docs`, limited to the `main` branch and `v*` tags, with:

   | Kind | Name | Value |
   |---|---|---|
   | Environment secret | `DOCS_SSH_KEY` | The private key, `quorium-docs-deploy` |
   | Environment secret | `DOCS_KNOWN_HOSTS` | The host key line the script printed |
   | Environment variable | `DOCS_USER` | `quorium-docs` |
   | Environment variable | `DOCS_DOMAIN` | The public domain, used for canonical URLs |
   | Environment variable | `DOCS_PORT` | The SSH port, if not 22 |

   Then, under Settings → Secrets and variables → Actions → Variables, add the **repository** variable `DOCS_HOST`: the SSH host name or address. It must be a repository variable, because the workflow reads it to decide whether to deploy before entering the environment.

4. Push to `main`, or run the `docs` workflow by hand on `main`, and open `https://docs.example.com/`.

Delete the private key from your machine once it is stored in GitHub. To rotate it, rerun the script with a new public key; it replaces the old one.

### Serve on a host and port

To serve the site from any machine without TLS, for example behind your own load balancer or on an internal network, install it as a systemd service:

```sh
sudo deploy/docs/install-service.sh --host 10.0.0.5 --port 8080 --source .
```

This installs Caddy, builds the documentation from the checkout given with `--source` into `/srv/quorium-docs/latest`, and starts the `quorium-docs` service, which serves `/srv/quorium-docs` on that address and port as the unprivileged `quorium-docs` user. It starts on boot and restarts on failure.

| Option | Default | Meaning |
|---|---|---|
| `--port PORT` | Required | The port to listen on. Ports below 1024 work |
| `--host HOST` | `0.0.0.0` | The address to listen on |
| `--root DIR` | `/srv/quorium-docs` | The directory to serve |
| `--source REPO_DIR` | None | A Quorium checkout to build the site from. Without it, the service serves whatever CI deployed to the root |

Rerun the script to change the address or port, or with `--source` to rebuild after pulling. If the `caddy` package's default site is running with its stock configuration, the script disables it, because it holds port 80. To stop the site: `sudo systemctl disable --now quorium-docs`.
