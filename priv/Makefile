OUTPUT=../ebin/torque_drv.so
LDFLAGS=-shared -fPIC -undefined -ltorque -lei -lerl_interface -L/usr/lib/erlang/lib/erl_interface-3.6.2/lib -L/opt/local/lib
CFLAGS=-o ${OUTPUT} -I/usr/lib/erlang/usr/include -I/usr/lib/erlang/lib/erl_interface-3.6.2/include/ -I/usr/local/include -I/opt/local/include

all:
	gcc torque_drv.c ${LDFLAGS} ${CFLAGS}
