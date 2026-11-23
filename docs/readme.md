# Releasing `useceleris-client`

The public Python package is published to PyPI from GitHub Actions with PyPI
Trusted Publishing. The workflow receives a short-lived PyPI credential only
after the `any` GitHub environment is approved. No PyPI API token belongs in
the repository, GitHub secrets or a developer machine.

## One-time setup

The PyPI project `useceleris-client` has a GitHub Actions trusted publisher
with these values:

| Setting | Value |
| --- | --- |
| Owner | `useceleris` |
| Repository | `sdk-py-client` |
| Workflow | `release.yml` |
| Environment | `any` |

The public GitHub repository has an `any` environment with required reviewer
approval. PyPI accepts any GitHub environment for this trusted publisher, so
the GitHub environment is the manual approval gate. Its name must remain
exactly `any`, because the workflow selects it. A publish job can wait for
approval for up to 30 days. If it expires, start a new release run; never
upload the version from a local machine.

## Branches

`main` is the normal development branch. It contains the release workflow so
GitHub can offer **Run workflow**. `release` is a protected branch that holds
the reviewed source for a production upload. Do not publish from `main` or a
feature branch.

Create a short-lived `release-prep/x.y.z` branch from `main` when preparing a
release. Its pull request goes directly to `release`; do not merge it into
`main` first.

## Prepare a release

1. Confirm that `main` contains the intended changes and that its CI run is
   green.
2. Create `release-prep/x.y.z` from `main`.
3. Set the final `x.y.z` version in both `pyproject.toml` and
   `src/useceleris_client/_constants.py`. They must match exactly and must be
   three numeric components, for example `1.1.0`.
4. Open and review a pull request from `release-prep/x.y.z` into `release`.
5. Merge the approved pull request.

## Publish

1. In GitHub Actions, open **Release**, click **Run workflow**, and select the
   `release` branch.
2. The workflow verifies the version, runs `uv run --locked nox`, then creates
   the wheel and source distribution that will be uploaded.
3. After those jobs pass, the `publish` job waits at the `any` environment.
   Review the run and select **Approve and deploy**.
4. The approved job uploads the already-built artifacts to PyPI using Trusted
   Publishing.
5. Confirm the package is available, then tag the exact `release` commit and
   create a GitHub Release:

   ```sh
   git switch release
   git pull --ff-only
   git status --short
   git tag -a v1.1.0 -m "v1.1.0"
   git push origin v1.1.0
   ```

The tag is the audit record for the immutable PyPI version. Do not rerun a
workflow after PyPI has accepted that version: PyPI versions cannot be
replaced. Correct a bad release with a new version.
