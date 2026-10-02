# Security policy

## Reporting a vulnerability

**Please do not open a public issue for a security problem.**

Report it privately through GitHub's [Security Advisories](https://github.com/useceleris/sdk-py-client/security/advisories/new) on this repository. That channel is private between you and the maintainer, and it lets us prepare a fix before anything is disclosed.

Include what you need to make the problem reproducible: the affected version, the Python version you saw it on, and the smallest example that shows it. A suggested fix is welcome but not required.

You should get an acknowledgement within a few days. We will tell you what we found, what we intend to do, and when we expect a fix to land, and we will credit you when it is published unless you would rather we did not.

## Supported versions

Fixes land on the latest release. There is no long-term support branch.

## Scope

In scope: anything in this package that lets one connection read, write or impersonate beyond what its credentials grant, any leak of a credential into a place it should not reach, and any input from the network that can crash or corrupt a consuming application.

Out of scope: the Celeris service itself, which is reported through the same channel on its own repository, and findings that require an attacker who already holds the signing secret. That secret is the trust boundary, and its compromise is total by design.

## What this package promises

This is the client package. It **never signs anything**: it contains no signing facility and no secret-taking constructor, and it does not depend on `useceleris-server`. The package check builds the wheel and asserts this on every run.

Bytes arriving from the network are treated as untrusted: the decoder is bounded on size, depth and fragment count, identifiers are validated, and no server-supplied text or credential is ever placed into an error message. Credentials are left out of `repr()`, and the WebSocket library's own logging is off for the client's connections, because it would log the credential URL. TLS certificates are verified against the system trust store.
