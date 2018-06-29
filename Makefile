PROTO_REPO = liftbridge-grpc

.PHONY: install
install: clean
	git clone git@github.com:tylertreat/$(PROTO_REPO).git
	go generate

clean:
	rm -rf $(PROTO_REPO)
