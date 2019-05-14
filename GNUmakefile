TOP_DIR := $(patsubst %/,%,$(realpath $(dir $(lastword $(MAKEFILE_LIST)))))
export GOPATH := $(TOP_DIR)

-include Local.mk

PREFERRED_INTERACTIVE_SHELL ?= bash
GO ?= go
GO_TEST_ARGS ?= "-count=1"
MAKE_SHELL_PS1 ?= 'pala $$ '
.PHONY: shell
ifeq ($(PREFERRED_INTERACTIVE_SHELL),bash)
shell:
	@INIT_FILE=$(shell mktemp); \
	printf '[ -e $$HOME/.bashrc ] && source $$HOME/.bashrc\n' > $$INIT_FILE; \
	printf '[ -e Local.env ] && source Local.env\n' >> $$INIT_FILE; \
	printf 'PS1='"$(MAKE_SHELL_PS1) "'\n' >> $$INIT_FILE; \
	$(PREFERRED_INTERACTIVE_SHELL) --init-file $$INIT_FILE || true
else ifeq ($(PREFERRED_INTERACTIVE_SHELL),fish)
shell:
	@INIT_FILE=$(shell mktemp); \
	printf 'if functions -q fish_right_prompt\n' > $$INIT_FILE; \
	printf '    functions -c fish_right_prompt __fish_right_prompt_original\n' >> $$INIT_FILE; \
	printf '    functions -e fish_right_prompt\n' >> $$INIT_FILE; \
	printf 'else\n' >> $$INIT_FILE; \
	printf '    function __fish_right_prompt_original\n' >> $$INIT_FILE; \
	printf '    end\n' >> $$INIT_FILE; \
	printf 'end\n' >> $$INIT_FILE; \
	printf 'function fish_right_prompt\n' >> $$INIT_FILE; \
	printf '    echo -n "(hodl) "\n' >> $$INIT_FILE; \
	printf '    __fish_right_prompt_original\n' >> $$INIT_FILE; \
	printf 'end\n' >> $$INIT_FILE; \
	$(PREFERRED_INTERACTIVE_SHELL) --init-command="source $$INIT_FILE" || true
else
shell:
	@$(PREFERRED_INTERACTIVE_SHELL) || true
endif

THUNDER2_PKGS := $(shell GOPATH=$(GOPATH) $(GO) list thunder2/...)
.PHONY: test
test:
	$(GO) test $(GO_TEST_ARGS) $(THUNDER2_PKGS)

.PHONY: dep
dep: src/thunder2/Gopkg.dep

%.dep: %.toml
	@md5sum $< > $@.cur
	@cmp -s $@.cur $@ || ( cd $(@D); echo "Updating $@" && dep ensure ); mv -f $@.cur $@

.PHONY: clean-dep
clean-dep:
	rm -rf $(TOP_DIR)/src/thunder2/vendor $(TOP_DIR)/src/thunder2/Gopkg.dep

.PHONY: lint
lint:
	golangci-lint run 2>golangci-lint.log
