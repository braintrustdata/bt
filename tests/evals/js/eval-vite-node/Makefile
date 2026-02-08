run:
	@for test in tests/*.eval.ts; do \
		echo "Running $$test with vite-node"; \
		bt eval --runner ./node_modules/.bin/vite-node "$$test"; \
	done
