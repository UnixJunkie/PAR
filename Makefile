clean:
	rm -f Pyro_NS_URI test_parallel.output nohup.out
	find . -name *.pyc -exec rm -f {} \;

test:
	./test_parallel.sh

check:
	pychecker src/*.py

dist: clean
	cd .. && tar czf par.tgz par

install:
	mkdir -p ~/bin && cp bin/par.sh ~/bin/par
