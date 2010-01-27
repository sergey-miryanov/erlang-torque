
all: compile

compile:
	erl -make
	cd priv && make

clean:
	rm -rf ./ebin/*.beam
	rm -rf ./ebin/*.so
	rm -rf ./*.beam

run:
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib erl -pa ./ebin -sname zerg

ex:
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib	./examples/run_job.erl
	LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib	./examples/wait_job.erl
