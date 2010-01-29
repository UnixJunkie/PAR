clean:
	rm -f src/*.pyc Pyro_NS_URI test_parallel.output

test:
	./test_parallel.sh

check:
	pychecker src/*.py
