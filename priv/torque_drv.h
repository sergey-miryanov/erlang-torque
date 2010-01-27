#include <pbs_config.h>
#include <pbs_ifl.h>
#include <pbs_error.h>
#include <erl_driver.h>
#include <ei.h>
#include <erl_interface.h>

typedef struct torque_drv_t {
    unsigned int  key;
    ErlDrvPort    port;
    FILE          *log;
    int           pbs_connect;
} torque_drv_t;

static ErlDrvData
start (ErlDrvPort port, 
       char *cmd);

static void
stop (ErlDrvData drv);

static int
control (ErlDrvData drv,
         unsigned int command,
         char *buf,
         int len,
         char **rbuf,
         int rlen);

static void
ready_async (ErlDrvData drv,
             ErlDrvThreadData thread_data);

static int
torque_connect (torque_drv_t *drv, char *server);
