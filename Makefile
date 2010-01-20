clean:
	rm -f *.pyc Pyro_NS_URI test_parallel.output

test:
	./test_parallel.sh

check:
	pychecker Data.py
	pychecker DataManager.py
	pychecker MetaDataManager.py
	pychecker ProgressBar.py
	pychecker parallel.py
	pychecker parallel.py
	pychecker post_proc_example.py
