.PHONY: all compile run test doc clean

REBAR=$(shell which rebar3 || echo ./rebar3)
MINIMAL_COVERAGE=80

all: compile

compile: $(REBAR)
	$(REBAR) compile

run: $(REBAR)
	@$(REBAR) as test shell --apps pooler --config config/demo.config

eunit: $(REBAR)
	$(REBAR) eunit --verbose --cover

proper: $(REBAR)
	$(REBAR) proper --cover

cover: $(REBAR)
	$(REBAR) cover --verbose --min_coverage $(MINIMAL_COVERAGE)

test: eunit proper cover

xref: $(REBAR)
	$(REBAR) xref

format: $(REBAR)
	$(REBAR) fmt

format_check: $(REBAR)
	$(REBAR) fmt --check

doc: $(REBAR)
	$(REBAR) edoc

clean: $(REBAR)
	$(REBAR) clean
	$(REBAR) as test clean
	@rm -rf ./erl_crash.dump

dialyzer: $(REBAR)
	$(REBAR) dialyzer

# Get rebar3 if it doesn't exist. If rebar3 was found on PATH, the
# $(REBAR) dep will be satisfied since the file will exist.

REBAR_URL = https://s3.amazonaws.com/rebar3/rebar3

./rebar3:
	@echo "Fetching rebar3 from $(REBAR_URL)"
	@erl -noinput -noshell -s inets -s ssl  -eval '{ok, _} = httpc:request(get, {"${REBAR_URL}", []}, [], [{stream, "${REBAR}"}])' -s init stop
		chmod +x ${REBAR}
