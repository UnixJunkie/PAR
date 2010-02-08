clean:
	rm -f src/*.pyc Pyro_NS_URI test_parallel.output nohup.out

test:
	./test_parallel.sh

check:
	pychecker src/*.py

dist: clean
	cd .. && tar czf par.tgz par
