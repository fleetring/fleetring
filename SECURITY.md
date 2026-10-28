# Security policy

## Supported versions

FleetRing is in early development. Security fixes go into the latest release of the `fleetring` crate.

## Reporting a vulnerability

Report vulnerabilities privately through GitHub: open the repository's **Security** tab and choose **Report a vulnerability**. Please don't open a public issue.

Include what you found, how to reproduce it, and the affected versions. We aim to acknowledge reports within a week.

Input that reaches `fleetring` from outside, such as a ring spec decoded from JSON, must never cause a panic. A crash on such input counts as a security bug.
