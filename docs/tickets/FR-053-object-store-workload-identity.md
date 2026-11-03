---
id: FR-053
title: Object-store credentials from workload identity, not only static keys
status: todo
priority: P2
size: S
milestone: M9 Archives and verification
depends_on: [FR-049]
adrs: [ADR-0029]
---

# FR-053: Object-store credentials from workload identity, not only static keys

## Goal

FR-049 v1 reads static access keys from the configuration or the environment. Deployments on Kubernetes and in the cloud usually forbid long-lived keys and hand pods short-lived credentials instead: AWS web-identity tokens (IRSA, EKS Pod Identity), GCP and Azure workload identity through their S3-compatible endpoints, or instance metadata. This ticket adds those sources without changing the archive format.

## Scope

- Credential sources in order: static keys (today), the AWS web-identity token file (`AWS_WEB_IDENTITY_TOKEN_FILE` plus `AWS_ROLE_ARN`, exchanged through STS `AssumeRoleWithWebIdentity`), the EKS Pod Identity agent, and instance metadata (IMDSv2 only). Each is enabled explicitly in configuration, and the first that is configured is used.
- Credentials refresh before they expire, with the time read from `tokio::time` (ADR-0020); a refresh failure is retried and shown in status, and archiving resumes when it succeeds.
- No new heavy dependency: the STS call is a signed HTTP request over the daemon's existing HTTP and TLS stack, like `rusty-s3`'s requests.
- Credentials never appear in logs, status, or `Debug` output.

## Acceptance criteria

- [ ] An archive authenticates with a web-identity token against a test STS endpoint, and keeps working across a credential rotation.
- [ ] IMDSv2 credentials work, and IMDSv1 is never used.
- [ ] `deploy/fleetringd.toml` documents each source.

## Progress

- Not started.
