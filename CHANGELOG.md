# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

- chore: streamline the workflow steps (#22)

### Added

- `apalis-board` support
- Workflow support
- Event driven listener

### Changed

- Moved from monorepo

## [0.7.4] - 2025-05-14

### 🐛 Bug Fixes

- *(sqlite)* Handle empty ID list in fetch_next to yield None, that make Event::Idle never trigger (#571)
## [0.7.1] - 2025-04-23

### 🐛 Bug Fixes

- Improve sqlite queries with transactions and single statement queries (#549)
## [0.7.0] - 2025-03-24

### 🐛 Bug Fixes

- Reenqueue oprphaned before starting streaming (#507)
- Ease apalis-core default features (#538)

### 💼 Other

- Generic retry persist check (#498)
- Add associated types to the `Backend` trait (#516)
## [0.6.4] - 2024-12-03

### 🐛 Bug Fixes

- Allow polling only when worker is ready (#472)
## [0.5.5] - 2024-07-03

### ⚙️ Miscellaneous Tasks

- Fix typos (#346)
## [0.4.9] - 2024-01-03

### 🚀 Features

- Configurable worker set as dead (#220)
## [0.4.7] - 2023-11-15

### 🐛 Bug Fixes

- Allow cargo build --all-features (#204)
## [0.4.5] - 2023-10-08

### 💼 Other

- Api to get migrations
## [0.4.4] - 2023-07-31

### 💼 Other

- Sqlx to v0.7
## [0.3.0] - 2022-06-05



Previous Document: https://github.com/apalis-dev/apalis/blob/main/CHANGELOG.md
