# Releases this package to PyPI. Run from the repository root, with the .venv
# from the README:
#
#   make release-test   rehearse on TestPyPI
#   make release        publish to PyPI; needs a clean git tree
#   make smoke          install the published version in a fresh venv
#   make dist           build and check without uploading (optional)
#
# Both uploads run `make dist` first, which runs the full check (nox), then
# builds and checks the files; nothing is uploaded if any step fails.
#
# Twine asks for the API token unless TWINE_PASSWORD is set; a TestPyPI token
# is separate from a PyPI one.

NAME := $(shell sed -n 's/^name = "\(.*\)"/\1/p' pyproject.toml)
VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' pyproject.toml)
MODULE := $(subst -,_,$(NAME))
PYTHON := .venv/bin/python

export TWINE_USERNAME ?= __token__

.PHONY: check dist committed release-test release smoke

check:
	$(PYTHON) -m pip install --quiet --group dev --group release
	$(PYTHON) -m nox

dist: check
	rm -rf dist
	$(PYTHON) -m build
	$(PYTHON) -m twine check --strict dist/*

committed:
	@test -z "$$(git status --porcelain)" || { echo "Commit or stash your changes first: a release must match a commit."; exit 1; }

release-test: dist
	$(PYTHON) -m twine upload --repository testpypi dist/*

release: committed dist
	@echo "Uploading $(NAME) $(VERSION) to PyPI. A version can never be uploaded twice."
	$(PYTHON) -m twine upload dist/*

smoke:
	@environment=$$(mktemp -d) && \
	$(PYTHON) -m venv $$environment && \
	$$environment/bin/pip install --quiet --disable-pip-version-check --no-cache-dir "$(NAME)==$(VERSION)" && \
	$$environment/bin/python -c "import $(MODULE)" && \
	echo "$(NAME) $(VERSION) installs from PyPI and imports."; \
	status=$$?; rm -rf $$environment; exit $$status
