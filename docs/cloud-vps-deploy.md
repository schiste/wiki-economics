# Cloud VPS Deployment

This repository supports two runtime profiles from the same codebase:

- local development and operator use
- production deployment on Wikimedia Cloud VPS

The shared parts stay the same:

- Rust CLI
- Python patrol pipeline
- merged artifact contract under `output/`
- Observable production site build

The production differences are operational:

- the public site is static and read-only
- the dev-only admin API is not exposed
- batch refresh runs from a scheduled service
- code releases and output releases are versioned separately

## Production Model

Recommended layout:

```text
/srv/wiki-economics/
  app/
    releases/
    current -> releases/<timestamp-sha>
  data/
  output/
    releases/
    current -> releases/<timestamp-sha>
  site/
    releases/
    current -> releases/<timestamp-sha>
  state/
```

Key properties:

- code deploys are versioned and rollbackable
- output refreshes are versioned and rollbackable
- the public web root is a symlink swap, not an in-place rebuild
- local development keeps using `scripts/dev.sh`

## Files Added For Cloud VPS

- `deploy/cloud-vps/bootstrap.sh`
- `deploy/cloud-vps/deploy-release.sh`
- `deploy/cloud-vps/run-refresh.sh`
- `deploy/cloud-vps/rollback.sh`
- `deploy/cloud-vps/env.example`
- `deploy/cloud-vps/systemd/wiki-econ-refresh.service`
- `deploy/cloud-vps/systemd/wiki-econ-refresh.timer`
- `deploy/cloud-vps/nginx/wiki-economics.conf`

## First Boot

1. Clone the repository onto the VPS.
2. Run:

```sh
sudo ./deploy/cloud-vps/bootstrap.sh
```

3. Edit:

- `/etc/wiki-economics.env`
- `/etc/wiki-economics/wikis.txt`

At minimum, make sure the wiki list contains only the projects you actually
intend to refresh on that VPS.

`bootstrap.sh` installs the Rust toolchain for the dedicated `wiki-econ`
service user so that code deploys can run without building as `root`.

## Code Deployment

Deploy a new application release:

```sh
sudo -u wiki-econ -H ./deploy/cloud-vps/deploy-release.sh
```

This will:

- clone the configured Git ref into a new release directory
- build the Rust CLI in that release
- install frontend dependencies
- switch `app/current`
- rebuild the static site against the current published artifacts, if any

Code deploy does not rebuild the data pipeline by itself.

After the first deploy, you can also run the copy from the current release:

```sh
sudo -u wiki-econ -H /srv/wiki-economics/app/current/deploy/cloud-vps/deploy-release.sh
```

## Batch Refresh

Run a full refresh manually:

```sh
sudo systemctl start wiki-econ-refresh.service
```

That script:

- runs the shared `scripts/refresh.sh` flow from `app/current`
- writes a new output release under `output/releases/`
- builds a new static site release under `site/releases/`
- validates required artifacts and pages
- atomically switches `output/current` and `site/current`

It uses:

- the enabled wiki list from `/etc/wiki-economics/wikis.txt`
- the production settings from `/etc/wiki-economics.env`

## Scheduling

Install the `systemd` files:

```sh
sudo cp deploy/cloud-vps/systemd/wiki-econ-refresh.service /etc/systemd/system/
sudo cp deploy/cloud-vps/systemd/wiki-econ-refresh.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now wiki-econ-refresh.timer
```

The timer is intentionally conservative. Adjust the schedule only after you
measure how long a real refresh takes on your VPS.

## Public Web Serving

Use nginx to serve the built static site:

```sh
sudo cp deploy/cloud-vps/nginx/wiki-economics.conf /etc/nginx/sites-available/wiki-economics.conf
sudo ln -s /etc/nginx/sites-available/wiki-economics.conf /etc/nginx/sites-enabled/wiki-economics.conf
sudo nginx -t
sudo systemctl reload nginx
```

The provided config:

- serves `/srv/wiki-economics/site/current`
- blocks `/admin`
- serves Observable assets directly

## Rollback

Rollback is done by switching symlinks:

```sh
sudo -u wiki-econ -H ./deploy/cloud-vps/rollback.sh --app <release>
sudo -u wiki-econ -H ./deploy/cloud-vps/rollback.sh --output <release> --site <release>
```

You can roll code back independently from published artifacts.

## Safety Rules

- Do not expose `site/admin-server.cjs` publicly in production.
- Do not rebuild output in place.
- Do not use one directory for both versioned source scripts and live data artifacts.
- Keep the enabled wiki list narrow at first.
- Run manual deploy and rollback operations as the dedicated `wiki-econ` service user.
- Start with one VM and split web/batch later only if needed.

## Shared Local / VPS Contract

The deployment scripts are wrappers around the same shared entrypoints used
locally:

- `scripts/setup.sh`
- `scripts/dev.sh`
- `scripts/refresh.sh`
- `scripts/build-site.sh`

That is deliberate. The dual deployment model should change orchestration,
not the underlying pipeline logic or artifact format.
