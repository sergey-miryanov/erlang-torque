
all: compile

compile:
	test -d ebin || mkdir ebin
	erl -make
	cd priv && make

clean:
	rm -rf ./ebin/*.beam
	rm -rf ./ebin/*.so
	rm -rf ./*.beam

run:
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/opt/local/lib erl -pa ./ebin -sname zerg

ex:
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/opt/local/lib	./examples/job_stat.erl
