OUTPUT=../ebin/torque_drv.so
LDFLAGS=-shared -fPIC -undefined -ltorque -lei -lerl_interface -L/usr/lib/erlang/lib/erl_interface-3.6.2/lib 
CFLAGS=-o ${OUTPUT} -I/usr/lib/erlang/usr/include -I/usr/lib/erlang/lib/erl_interface-3.6.2/include/ -I/home/zerg/Downloads/torque-2.4.4/src/include

all:
	gcc torque_drv.c ${LDFLAGS} ${CFLAGS}
