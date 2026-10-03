# Releases this package to PyPI. Run from the repository root:
#
#   make release-test   rehearse on TestPyPI
#   make release        publish to PyPI; needs a clean git tree
#   make smoke          install the published version in a fresh environment
#   make dist           check and build without uploading (optional)
#
# Both uploads run `make dist` first, which runs the full check (nox) against
# the committed uv.lock, then builds; nothing is uploaded if any step fails.
#
# Unless UV_PUBLISH_TOKEN is set, uv asks for a username, then a password:
# enter __token__, then the API token. A TestPyPI token is separate from a
# PyPI one.

NAME := $(shell sed -n 's/^name = "\(.*\)"/\1/p' pyproject.toml)
VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml)
MODULE := $(subst -,_,$(NAME))

.PHONY: check dist committed release-test release smoke

check:
	uv run --locked nox

dist: check
	rm -rf dist
	uv build

committed:
	@test -z "$$(git status --porcelain)" || { echo "Commit or stash your changes first: a release must match a commit."; exit 1; }

release-test: dist
	uv publish --publish-url https://test.pypi.org/legacy/

release: committed dist
	@echo "Uploading $(NAME) $(VERSION) to PyPI. A version can never be uploaded twice."
	uv publish

# --prerelease allow matches the documented `pip install --pre`.
smoke:
	uv run --isolated --no-project --refresh --prerelease allow --with "$(NAME)==$(VERSION)" python -c "import $(MODULE)"
	@echo "$(NAME) $(VERSION) installs from PyPI and imports."
